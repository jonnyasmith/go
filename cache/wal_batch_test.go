package cache

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteBatchRollsBeforeRecordBeyondExactBoundary(t *testing.T) {
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
	if first.Size() != segmentHeaderSize+2*recordSize {
		t.Fatalf("first segment size = %d; want exact boundary %d", first.Size(), segmentHeaderSize+2*recordSize)
	}
	second, err := os.Stat(filepath.Join(dir, segmentName(3)))
	if err != nil {
		t.Fatalf("stat rollover segment: %v", err)
	}
	if second.Size() != segmentHeaderSize+recordSize {
		t.Fatalf("second segment size = %d; want %d", second.Size(), segmentHeaderSize+recordSize)
	}
	if log.seq != 3 {
		t.Fatalf("sequence = %d; want 3", log.seq)
	}
}

func TestAppendRecordUsesCallerStorage(t *testing.T) {
	request := &writeRequest{kind: requestSet, key: "mixed", value: []byte("value")}
	prefix := []byte("prefix")
	storage := make([]byte, len(prefix), len(prefix)+recordSizeFor(request))
	copy(storage, prefix)

	encoded := appendRecord(storage, 7, request)
	if string(encoded[:len(prefix)]) != string(prefix) {
		t.Fatalf("prefix = %q; want %q", encoded[:len(prefix)], prefix)
	}
	if len(encoded) != len(prefix)+recordSizeFor(request) {
		t.Fatalf("encoded length = %d; want %d", len(encoded), len(prefix)+recordSizeFor(request))
	}
}

func newTestSetRequest(key string) *writeRequest {
	return &writeRequest{kind: requestSet, key: key, result: make(chan error, 1)}
}
