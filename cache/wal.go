package cache

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	segmentHeaderSize = int64(8)
	recordFixedSize   = 4 + 4 + 1 + 2 + 8 + 8
	formatVersion     = uint16(1)
	opSet             = byte(1)
	opDelete          = byte(2)
)

var (
	segmentMagic = [4]byte{'C', 'W', 'A', 'L'}
	crcTable     = crc32.MakeTable(crc32.Castagnoli)
)

type requestKind uint8

const (
	requestSet requestKind = iota + 1
	requestDelete
	requestSync
	requestClose
)

type writeRequest struct {
	kind   requestKind
	key    string
	value  []byte
	result chan error
}

type logState struct {
	file   *os.File
	offset int64
	seq    uint64
}

func (store *Store) runWriter(log *logState, options options) {
	defer close(store.done)
	ticker := time.NewTicker(options.flushInterval)
	defer ticker.Stop()
	defer func() { _ = log.file.Close() }()

	for {
		select {
		case <-ticker.C:
			if store.stats.lastError.Load() == nil {
				if err := log.file.Sync(); err != nil {
					store.latch(fmt.Errorf("cache: fsync log: %w", err))
				} else {
					store.stats.fsyncs.Add(1)
				}
			}
		case request := <-store.requests:
			switch request.kind {
			case requestSet, requestDelete:
				batch := []*writeRequest{request}
			drain:
				for {
					select {
					case next := <-store.requests:
						if next.kind != requestSet && next.kind != requestDelete {
							store.writeBatch(log, options.segmentSize, batch)
							if store.handleControl(log, next) {
								return
							}
							break drain
						}
						batch = append(batch, next)
					default:
						store.writeBatch(log, options.segmentSize, batch)
						break drain
					}
				}
			case requestSync, requestClose:
				if store.handleControl(log, request) {
					return
				}
			}
		}
	}
}

func (store *Store) handleControl(log *logState, request *writeRequest) bool {
	failure := store.stats.lastError.Load()
	if failure != nil && request.kind != requestClose {
		request.result <- failure.err
		return false
	}

	var err error
	if failure != nil {
		err = failure.err
	}
	if syncErr := log.file.Sync(); syncErr != nil {
		syncErr = fmt.Errorf("cache: fsync log: %w", syncErr)
		if failure == nil {
			syncErr = store.latch(syncErr)
		}
		err = errors.Join(err, syncErr)
	} else {
		store.stats.fsyncs.Add(1)
	}
	if request.kind == requestClose {
		err = errors.Join(err, log.file.Close())
	}
	request.result <- err
	return request.kind == requestClose
}

func (store *Store) writeBatch(log *logState, segmentSize int64, requests []*writeRequest) {
	if failure := store.stats.lastError.Load(); failure != nil {
		respondAll(requests, failure.err)
		return
	}

	for first := 0; first < len(requests); {
		nextRecord := encodeRecord(log.seq+1, requests[first])
		if log.offset > segmentHeaderSize && log.offset+int64(len(nextRecord)) > segmentSize {
			if err := store.rollSegment(log, log.seq+1); err != nil {
				failure := store.latch(err)
				respondAll(requests[first:], failure)
				return
			}
		}

		payload := make([]byte, 0, len(nextRecord)*(len(requests)-first))
		last := first
		for last < len(requests) {
			record := encodeRecord(log.seq+uint64(last-first)+1, requests[last])
			if last > first && log.offset+int64(len(payload)) > segmentSize {
				break
			}
			payload = append(payload, record...)
			last++
		}

		written, err := log.file.Write(payload)
		if err == nil && written != len(payload) {
			err = io.ErrShortWrite
		}
		if err != nil {
			failure := store.latch(fmt.Errorf("cache: write log %q at offset %d: %w", log.file.Name(), log.offset, err))
			respondAll(requests[first:], failure)
			return
		}

		log.offset += int64(written)
		for index := first; index < last; index++ {
			log.seq++
			request := requests[index]
			if request.kind == requestSet {
				store.applySet(request.key, request.value)
			} else {
				store.applyDelete(request.key)
			}
			store.stats.recordsWritten.Add(1)
			request.result <- nil
		}
		store.stats.bytesWritten.Add(uint64(written))
		first = last
	}
}

func (store *Store) rollSegment(log *logState, firstSequence uint64) error {
	if err := log.file.Sync(); err != nil {
		return fmt.Errorf("cache: fsync segment %q: %w", log.file.Name(), err)
	}
	store.stats.fsyncs.Add(1)
	if err := log.file.Close(); err != nil {
		return fmt.Errorf("cache: close segment %q: %w", log.file.Name(), err)
	}
	file, err := createSegment(store.dir, firstSequence)
	if err != nil {
		return err
	}
	log.file = file
	log.offset = segmentHeaderSize
	return nil
}

func respondAll(requests []*writeRequest, err error) {
	for _, request := range requests {
		request.result <- err
	}
}

func encodeRecord(sequence uint64, request *writeRequest) []byte {
	length := recordFixedSize + len(request.key) + len(request.value)
	record := make([]byte, length)
	binary.LittleEndian.PutUint32(record[0:4], uint32(length-4))
	if request.kind == requestSet {
		record[8] = opSet
	} else {
		record[8] = opDelete
	}
	binary.LittleEndian.PutUint16(record[9:11], uint16(len(request.key)))
	binary.LittleEndian.PutUint64(record[19:27], sequence)
	copy(record[27:], request.key)
	copy(record[27+len(request.key):], request.value)
	binary.LittleEndian.PutUint32(record[4:8], crc32.Checksum(record[8:], crcTable))
	return record
}

func createSegment(dir string, firstSequence uint64) (*os.File, error) {
	path := filepath.Join(dir, segmentName(firstSequence))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("cache: create segment %q: %w", path, err)
	}
	header := make([]byte, segmentHeaderSize)
	copy(header[:4], segmentMagic[:])
	binary.LittleEndian.PutUint16(header[4:6], formatVersion)
	if _, err := file.Write(header); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("cache: write segment header %q: %w", path, err)
	}
	return file, nil
}

func segmentName(firstSequence uint64) string {
	return fmt.Sprintf("%08d.seg", firstSequence)
}
