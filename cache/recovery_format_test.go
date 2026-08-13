package cache_test

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cache "github.com/jonnyasmith/go/cache"
)

func TestUnsupportedSegmentHeadersRefuseOpen(t *testing.T) {
	tests := []struct {
		name   string
		header []byte
		want   string
	}{
		{name: "unknown magic", header: []byte{'B', 'A', 'D', '!', 1, 0, 0, 0}, want: "magic"},
		{name: "future version", header: []byte{'C', 'W', 'A', 'L', 2, 0, 0, 0}, want: "version"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "00000001.seg")
			if err := os.WriteFile(path, test.header, 0o600); err != nil {
				t.Fatalf("write segment: %v", err)
			}
			_, err := cache.Open(context.Background(), dir)
			if err == nil || !strings.Contains(err.Error(), path) || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("open error = %v; want path and %q", err, test.want)
			}
		})
	}
}

func TestSequenceGapRefusesOpenAtRecordOffset(t *testing.T) {
	dir := t.TempDir()
	store, err := cache.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, key := range []string{"a", "b"} {
		if err := store.Set(key, []byte("value")); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	path := filepath.Join(dir, "00000001.seg")
	segment, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	firstLength := int(binary.LittleEndian.Uint32(segment[8:12]))
	secondOffset := 8 + 4 + firstLength
	secondLength := int(binary.LittleEndian.Uint32(segment[secondOffset : secondOffset+4]))
	secondEnd := secondOffset + 4 + secondLength
	binary.LittleEndian.PutUint64(segment[secondOffset+19:secondOffset+27], 3)
	checksum := crc32.Checksum(segment[secondOffset+8:secondEnd], crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(segment[secondOffset+4:secondOffset+8], checksum)
	if err := os.WriteFile(path, segment, 0o600); err != nil {
		t.Fatalf("write segment with gap: %v", err)
	}

	_, err = cache.Open(context.Background(), dir)
	if err == nil || !strings.Contains(err.Error(), path) || !strings.Contains(err.Error(), "offset") || !strings.Contains(err.Error(), "sequence gap") {
		t.Fatalf("open error = %v; want path, offset, and sequence gap", err)
	}
}
