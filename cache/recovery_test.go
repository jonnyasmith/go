package cache_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	cache "github.com/jonnyasmith/go/cache"
)

func TestConcurrentWritesRecover(t *testing.T) {
	dir := t.TempDir()
	store, err := cache.Open(context.Background(), dir, cache.WithShards(8), cache.WithFlushInterval(time.Hour))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const writes = 256
	var wait sync.WaitGroup
	for index := range writes {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			key := fmt.Sprintf("key-%03d", index)
			if err := store.Set(key, []byte(key)); err != nil {
				t.Errorf("set %q: %v", key, err)
			}
		}(index)
	}
	wait.Wait()
	if store.Len() != writes {
		t.Fatalf("len = %d; want %d", store.Len(), writes)
	}
	if err := store.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if store.Stats().Fsyncs == 0 {
		t.Fatal("sync did not increment fsync count")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	recovered, err := cache.Open(context.Background(), dir, cache.WithShards(4))
	if err != nil {
		t.Fatalf("reopen with changed shard count: %v", err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	if recovered.Len() != writes {
		t.Fatalf("recovered len = %d; want %d", recovered.Len(), writes)
	}
	for index := range writes {
		key := fmt.Sprintf("key-%03d", index)
		value, ok := recovered.Get(key)
		if !ok || string(value) != key {
			t.Fatalf("get %q = %q, %v", key, value, ok)
		}
	}
}

func TestEveryTruncationWithinFinalRecordIsATornTail(t *testing.T) {
	base := t.TempDir()
	store, err := cache.Open(context.Background(), base)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.Set("final", []byte("record")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	segment, err := os.ReadFile(filepath.Join(base, "00000001.seg"))
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}

	const segmentHeaderSize = 8
	for cut := segmentHeaderSize; cut < len(segment); cut++ {
		t.Run(fmt.Sprintf("offset_%d", cut-segmentHeaderSize), func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "00000001.seg"), segment[:cut], 0o600); err != nil {
				t.Fatalf("write truncated segment: %v", err)
			}
			recovered, err := cache.Open(context.Background(), dir)
			if err != nil {
				t.Fatalf("open truncated segment: %v", err)
			}
			defer recovered.Close()
			if _, ok := recovered.Get("final"); ok {
				t.Fatal("incomplete final record was recovered")
			}
		})
	}
}

func TestInteriorChecksumFailureRefusesRecovery(t *testing.T) {
	dir := t.TempDir()
	store, err := cache.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.Set("first", []byte("one")); err != nil {
		t.Fatalf("set first: %v", err)
	}
	if err := store.Set("second", []byte("two")); err != nil {
		t.Fatalf("set second: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	path := filepath.Join(dir, "00000001.seg")
	segment, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	segment[8+8] ^= 0xff
	if err := os.WriteFile(path, segment, 0o600); err != nil {
		t.Fatalf("corrupt segment: %v", err)
	}
	_, err = cache.Open(context.Background(), dir)
	if err == nil || !containsAll(err.Error(), path, "offset", "CRC32C") {
		t.Fatalf("open error = %v; want path, offset, and CRC32C", err)
	}
}

func TestWriteFailureLatchesUntilReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := cache.Open(context.Background(), dir, cache.WithSegmentSize(9))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.Set("live", []byte("value")); err != nil {
		t.Fatalf("first set: %v", err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("make directory read-only: %v", err)
	}
	firstErr := store.Set("rejected", []byte("value"))
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("restore directory permissions: %v", err)
	}
	if firstErr == nil {
		t.Fatal("set after rollover unexpectedly succeeded")
	}
	secondErr := store.Delete("live")
	if secondErr != firstErr {
		t.Fatalf("latched error = %v; want same error value %v", secondErr, firstErr)
	}
	value, ok := store.Get("live")
	if !ok || string(value) != "value" {
		t.Fatalf("read after failure = %q, %v", value, ok)
	}
	if store.Stats().LastError == "" {
		t.Fatal("last error was not reported")
	}
	if err := store.Close(); err == nil {
		t.Fatal("close after durability failure unexpectedly succeeded")
	}

	reopened, err := cache.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if err := reopened.Set("new", []byte("value")); err != nil {
		t.Fatalf("set after reopen: %v", err)
	}
}

func TestSegmentRolloverPreservesSequenceForRecovery(t *testing.T) {
	dir := t.TempDir()
	store, err := cache.Open(context.Background(), dir, cache.WithSegmentSize(40))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, key := range []string{"a", "b", "c"} {
		if err := store.Set(key, []byte(key)); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
	}

	for _, name := range []string{"00000001.seg", "00000002.seg", "00000003.seg"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	recovered, err := cache.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	for _, key := range []string{"a", "b", "c"} {
		value, ok := recovered.Get(key)
		if !ok || string(value) != key {
			t.Fatalf("get %q = %q, %v", key, value, ok)
		}
	}
}

func TestCanceledRecoveryContextStopsOpen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := cache.Open(ctx, t.TempDir())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("open error = %v; want context.Canceled", err)
	}
}

func containsAll(value string, parts ...string) bool {
	for _, part := range parts {
		if !strings.Contains(value, part) {
			return false
		}
	}
	return true
}
