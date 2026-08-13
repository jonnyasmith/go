package cache_test

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cache "github.com/jonnyasmith/go/cache"
)

func TestAutomaticSnapshotBoundsSegmentsAndRecovers(t *testing.T) {
	dir := t.TempDir()
	store, err := cache.Open(context.Background(), dir,
		cache.WithShards(2),
		cache.WithSegmentSize(48),
		cache.WithSnapshotThreshold(64),
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for index := range 20 {
		key := string(rune('a' + index))
		if err := store.Set(key, []byte("value")); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	snapshots, err := filepath.Glob(filepath.Join(dir, "*.snap"))
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("snapshot count = %d; want 1", len(snapshots))
	}
	segments, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	if len(segments) > 2 {
		t.Fatalf("retained segment count = %d; want at most 2", len(segments))
	}

	recovered, err := cache.Open(context.Background(), dir, cache.WithShards(2))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	for index := range 20 {
		key := string(rune('a' + index))
		value, ok := recovered.Get(key)
		if !ok || string(value) != "value" {
			t.Fatalf("get %q = %q, %v", key, value, ok)
		}
	}
}

func TestCloseInstallsFinalSnapshot(t *testing.T) {
	dir := t.TempDir()
	store, err := cache.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.Set("key", []byte("value")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	snapshots, err := filepath.Glob(filepath.Join(dir, "*.snap"))
	if err != nil {
		t.Fatalf("list snapshots: %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("snapshot count = %d; want 1", len(snapshots))
	}
}

func TestSnapshotWithDifferentShardCountIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	store, err := cache.Open(context.Background(), dir,
		cache.WithShards(2),
		cache.WithSegmentSize(48),
		cache.WithSnapshotThreshold(1),
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for index := range 10 {
		key := string(rune('a' + index))
		if err := store.Set(key, []byte("value")); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	recovered, err := cache.Open(context.Background(), dir, cache.WithShards(4))
	if err != nil {
		t.Fatalf("reopen with different shard count: %v", err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	for index := range 10 {
		key := string(rune('a' + index))
		value, ok := recovered.Get(key)
		if !ok || string(value) != "value" {
			t.Fatalf("get %q = %q, %v; want value, true", key, value, ok)
		}
	}
}

func TestFutureSnapshotVersionRefusesOpen(t *testing.T) {
	dir := t.TempDir()
	store, err := cache.Open(context.Background(), dir, cache.WithSnapshotThreshold(1))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.Set("key", []byte("value")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	snapshots, err := filepath.Glob(filepath.Join(dir, "*.snap"))
	if err != nil || len(snapshots) != 1 {
		t.Fatalf("list snapshot = %v, %v", snapshots, err)
	}
	image, err := os.ReadFile(snapshots[0])
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	binary.LittleEndian.PutUint16(image[4:6], 2)
	if err := os.WriteFile(snapshots[0], image, 0o600); err != nil {
		t.Fatalf("corrupt snapshot version: %v", err)
	}

	_, err = cache.Open(context.Background(), dir)
	if err == nil || !strings.Contains(err.Error(), snapshots[0]) || !strings.Contains(err.Error(), "version") {
		t.Fatalf("open error = %v; want snapshot path and version", err)
	}
}

func TestInterruptedSnapshotIsIgnored(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".snapshot-interrupted"), []byte("partial"), 0o600); err != nil {
		t.Fatalf("write temporary snapshot: %v", err)
	}
	store, err := cache.Open(context.Background(), dir, cache.WithSnapshotThreshold(1))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.Set("key", []byte("value")); err != nil {
		t.Fatalf("set: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for store.Stats().Snapshots == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if store.Stats().Snapshots == 0 {
		t.Fatal("automatic snapshot was not taken")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
