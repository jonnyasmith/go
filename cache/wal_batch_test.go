package cache

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteBatchPreservesRolloverAfterExceededBoundary(t *testing.T) {
	dir := t.TempDir()
	store := newStoreState(dir, nil, 1, 1<<20, nil)
	file, err := store.createSegment(1)
	if err != nil {
		t.Fatalf("create first segment: %v", err)
	}
	log := &logState{file: file, offset: segmentHeaderSize}
	requests := []*writeRequest{
		newTestSetRequest("a"),
		newTestSetRequest("b"),
		newTestSetRequest("c"),
		newTestSetRequest("d"),
	}

	const recordSize = recordFixedSize + 1
	store.writeBatch(log, segmentHeaderSize+2*recordSize, requests)
	for index, request := range requests {
		if err := <-request.result; err != nil {
			t.Fatalf("request %d: %v", index, err)
		}
	}
	if err := log.file.Close(); err != nil {
		t.Fatalf("close final segment: %v", err)
	}

	first, err := os.Stat(filepath.Join(dir, segmentName(1)))
	if err != nil {
		t.Fatalf("stat first segment: %v", err)
	}
	if first.Size() != segmentHeaderSize+3*recordSize {
		t.Fatalf("first segment size = %d; want one record beyond boundary %d", first.Size(), segmentHeaderSize+3*recordSize)
	}
	second, err := os.Stat(filepath.Join(dir, segmentName(4)))
	if err != nil {
		t.Fatalf("stat rollover segment: %v", err)
	}
	if second.Size() != segmentHeaderSize+recordSize {
		t.Fatalf("second segment size = %d; want %d", second.Size(), segmentHeaderSize+recordSize)
	}
	if log.seq != 4 {
		t.Fatalf("sequence = %d; want 4", log.seq)
	}
}

func TestAppendRecordUsesCallerStorage(t *testing.T) {
	request := &writeRequest{kind: requestSet, key: "mixed", value: []byte("value")}
	prefix := []byte("prefix")
	storage := make([]byte, len(prefix), len(prefix)+recordSizeFor(request))
	copy(storage, prefix)

	encoded := appendRecord(storage, 7, request)
	if &encoded[0] != &storage[0] {
		t.Fatal("encoded record did not reuse caller storage")
	}
	if string(encoded[:len(prefix)]) != string(prefix) {
		t.Fatalf("prefix = %q; want %q", encoded[:len(prefix)], prefix)
	}
	if len(encoded) != len(prefix)+recordSizeFor(request) {
		t.Fatalf("encoded length = %d; want %d", len(encoded), len(prefix)+recordSizeFor(request))
	}
	record := encoded[len(prefix):]
	if length := binary.LittleEndian.Uint32(record[:recordCRCOffset]); length != uint32(len(record)-segmentLengthSize) {
		t.Fatalf("record length = %d; want %d", length, len(record)-segmentLengthSize)
	}
	checksum := binary.LittleEndian.Uint32(record[recordCRCOffset:recordOpOffset])
	if want := crc32.Checksum(record[recordOpOffset:], crcTable); checksum != want {
		t.Fatalf("record checksum = %d; want %d", checksum, want)
	}
}

func encodeRecord(sequence uint64, request *writeRequest) []byte {
	return appendRecord(nil, sequence, request)
}

func newTestSetRequest(key string) *writeRequest {
	return &writeRequest{kind: requestSet, key: key, result: make(chan error, 1)}
}
