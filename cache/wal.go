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
	segmentHeaderSize     = int64(8)
	segmentLengthSize     = 4
	recordCRCOffset       = 4
	recordOpOffset        = 8
	recordKeyLengthOffset = 9
	recordDeadlineOffset  = 11
	recordSequenceOffset  = 19
	recordKeyOffset       = 27
	recordFixedSize       = recordKeyOffset
	formatVersion         = uint16(1)
	opSet                 = byte(1)
	opDelete              = byte(2)
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
	kind     requestKind
	key      string
	value    []byte
	deadline int64
	result   chan error
}

type logState struct {
	file               *os.File
	offset             int64
	seq                uint64
	bytesSinceSnapshot int64
}

func (store *Store) runWriter(log *logState, options options) {
	defer close(store.done)
	ticker := time.NewTicker(options.flushInterval)
	flushStop := store.flushStop
	defer func() { _ = log.file.Close() }()
	for {
		select {
		case <-flushStop:
			ticker.Stop()
			close(store.flushDone)
			flushStop = nil
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
				batch, control := store.drainWrites(request)
				store.writeBatch(log, options.segmentSize, batch)
				if log.bytesSinceSnapshot >= options.snapshotThreshold && store.startSnapshot() {
					log.bytesSinceSnapshot = 0
				}
				if control != nil && store.handleControl(log, control) {
					return
				}
			case requestSync, requestClose:
				if store.handleControl(log, request) {
					return
				}
			}
		}
	}
}

func (store *Store) drainWrites(first *writeRequest) ([]*writeRequest, *writeRequest) {
	batch := []*writeRequest{first}
	for {
		select {
		case request := <-store.requests:
			if request.kind == requestSync || request.kind == requestClose {
				return batch, request
			}
			batch = append(batch, request)
		default:
			return batch, nil
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
		store.snapshotWG.Wait()
		if snapshotErr := store.writeFinalSnapshot(); snapshotErr != nil {
			store.stats.lastSnapshotError.Store(&errorState{err: snapshotErr})
			err = errors.Join(err, snapshotErr)
		}
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
		if log.seq == ^uint64(0) {
			failure := store.latch(fmt.Errorf("cache: WAL sequence space exhausted"))
			respondAll(requests[first:], failure)
			return
		}
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
			if uint64(last-first) >= ^uint64(0)-log.seq {
				break
			}
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
		log.bytesSinceSnapshot += int64(written)
		for index := first; index < last; index++ {
			log.seq++
			request := requests[index]
			if request.kind == requestSet {
				store.applySet(request.key, request.value, request.deadline, log.seq)
			} else {
				store.applyDelete(request.key, log.seq)
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
	file, err := store.createSegment(firstSequence)
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
	binary.LittleEndian.PutUint32(record[:recordCRCOffset], uint32(length-segmentLengthSize))
	if request.kind == requestSet {
		record[recordOpOffset] = opSet
	} else {
		record[recordOpOffset] = opDelete
	}
	binary.LittleEndian.PutUint16(record[recordKeyLengthOffset:recordDeadlineOffset], uint16(len(request.key)))
	binary.LittleEndian.PutUint64(record[recordDeadlineOffset:recordSequenceOffset], uint64(request.deadline))
	binary.LittleEndian.PutUint64(record[recordSequenceOffset:recordKeyOffset], sequence)
	copy(record[recordKeyOffset:], request.key)
	copy(record[recordKeyOffset+len(request.key):], request.value)
	binary.LittleEndian.PutUint32(record[recordCRCOffset:recordOpOffset], crc32.Checksum(record[recordOpOffset:], crcTable))
	return record
}

func (store *Store) createSegment(firstSequence uint64) (*os.File, error) {
	path := filepath.Join(store.dir, segmentName(firstSequence))
	file, err := os.CreateTemp(store.dir, ".segment-*")
	if err != nil {
		return nil, fmt.Errorf("cache: create segment temporary file for %q: %w", path, err)
	}
	temporaryPath := file.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("cache: set segment permissions %q: %w", temporaryPath, err)
	}
	header := make([]byte, segmentHeaderSize)
	copy(header[:4], segmentMagic[:])
	binary.LittleEndian.PutUint16(header[4:6], formatVersion)
	if _, err := file.Write(header); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("cache: write segment header %q: %w", temporaryPath, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("cache: sync segment header %q: %w", temporaryPath, err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("cache: install segment %q: %w", path, err)
	}
	removeTemporary = false
	if err := store.directorySync(store.dir); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("cache: commit segment installation %q: %w", path, err)
	}
	return file, nil
}

func segmentName(firstSequence uint64) string {
	return fmt.Sprintf("%020d.seg", firstSequence)
}
