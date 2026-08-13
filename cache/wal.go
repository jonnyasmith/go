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
	maxSequence           = ^uint64(0)
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
				if err := store.files.Load().syncWAL(log.file); err != nil {
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
				if log.bytesSinceSnapshot >= options.snapshotThreshold && store.startSnapshot(log) {
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
	if syncErr := store.files.Load().syncWAL(log.file); syncErr != nil {
		syncErr = fmt.Errorf("cache: fsync log: %w", syncErr)
		if failure == nil {
			err = store.latch(syncErr)
		} else {
			err = errors.Join(err, syncErr)
		}
	} else {
		store.stats.fsyncs.Add(1)
	}
	if request.kind == requestClose {
		store.snapshotWG.Wait()
		if snapshotErr := store.writeFinalSnapshot(log.seq); snapshotErr != nil {
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
		if log.seq == maxSequence {
			failure := store.latch(fmt.Errorf("cache: WAL sequence space exhausted"))
			respondAll(requests[first:], failure)
			return
		}
		firstSize := recordSizeFor(requests[first])
		if log.offset > segmentHeaderSize && log.offset+int64(firstSize) > segmentSize {
			if err := store.rollSegment(log, log.seq+1); err != nil {
				failure := store.latch(err)
				respondAll(requests[first:], failure)
				return
			}
		}

		payloadSize := 0
		last := first
		for last < len(requests) {
			if uint64(last-first) >= maxSequence-log.seq {
				break
			}
			size := recordSizeFor(requests[last])
			if last > first && log.offset+int64(payloadSize)+int64(size) > segmentSize {
				break
			}
			payloadSize += size
			last++
		}

		payload := make([]byte, 0, payloadSize)
		for index := first; index < last; index++ {
			payload = appendRecord(payload, log.seq+uint64(index-first)+1, requests[index])
		}
		written, err := store.files.Load().writeWAL(log.file, payload)
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
	if err := store.files.Load().syncWAL(log.file); err != nil {
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
	return appendRecord(nil, sequence, request)
}

func recordSizeFor(request *writeRequest) int {
	return recordFixedSize + len(request.key) + len(request.value)
}

func appendRecord(dst []byte, sequence uint64, request *writeRequest) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, recordSizeFor(request))...)
	record := dst[start:]
	binary.LittleEndian.PutUint32(record[:recordCRCOffset], uint32(len(record)-segmentLengthSize))
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
	return dst
}

func (store *Store) createSegment(firstSequence uint64) (*os.File, error) {
	path := filepath.Join(store.dir, segmentName(firstSequence))
	files := store.files.Load()
	file, err := files.createTemp(store.dir, ".segment-*")
	if err != nil {
		return nil, fmt.Errorf("cache: create segment temporary file for %q: %w", path, err)
	}
	temporaryPath := file.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = files.remove(temporaryPath)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("cache: set segment permissions %q: %w", temporaryPath, err)
	}
	header := make([]byte, segmentHeaderSize)
	copy(header[:4], segmentMagic[:])
	binary.LittleEndian.PutUint16(header[4:6], formatVersion)
	if _, err := files.writeWAL(file, header); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("cache: write segment header %q: %w", temporaryPath, err)
	}
	if err := files.syncWAL(file); err != nil {
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
