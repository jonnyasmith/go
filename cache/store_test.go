package cache_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	cache "github.com/jonnyasmith/go/cache"
)

func TestStorePersistsCopiedValues(t *testing.T) {
	dir := t.TempDir()
	store, err := cache.Open(context.Background(), dir, cache.WithFlushInterval(time.Hour))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	input := []byte("value")
	if err := store.Set("key", input); err != nil {
		t.Fatalf("set: %v", err)
	}
	input[0] = 'X'

	got, ok := store.Get("key")
	if !ok || string(got) != "value" {
		t.Fatalf("get = %q, %v; want value, true", got, ok)
	}
	got[0] = 'Y'
	got, ok = store.Get("key")
	if !ok || string(got) != "value" {
		t.Fatalf("get after mutation = %q, %v; want value, true", got, ok)
	}

	dst := make([]byte, 1, 16)
	dst, ok = store.GetInto("key", dst)
	if !ok || string(dst) != "value" {
		t.Fatalf("get into = %q, %v; want value, true", dst, ok)
	}
	if store.Len() != 1 || store.Bytes() != uint64(64+len("key")+len("value")) {
		t.Fatalf("size = %d entries, %d bytes", store.Len(), store.Bytes())
	}

	stats := store.Stats()
	if stats.Hits != 3 || stats.RecordsWritten != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := store.Set("closed", nil); !errors.Is(err, cache.ErrClosed) {
		t.Fatalf("set after close = %v; want ErrClosed", err)
	}

	store, err = cache.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	got, ok = store.Get("key")
	if !ok || string(got) != "value" {
		t.Fatalf("recovered get = %q, %v; want value, true", got, ok)
	}
	if err := store.Delete("key"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := store.Delete("absent"); err != nil {
		t.Fatalf("delete absent: %v", err)
	}
	if _, ok := store.Get("key"); ok {
		t.Fatal("deleted key is present")
	}
}

func TestOpenRejectsInvalidOptions(t *testing.T) {
	ctx := context.Background()
	for name, option := range map[string]cache.Option{
		"WithShards":        cache.WithShards(3),
		"WithCapacity":      cache.WithCapacity(0),
		"WithFlushInterval": cache.WithFlushInterval(0),
		"WithSegmentSize":   cache.WithSegmentSize(0),
		"WithSweepInterval": cache.WithSweepInterval(0),
		"WithLogger":        cache.WithLogger(nil),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := cache.Open(ctx, t.TempDir(), option)
			if err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("open error = %v; want option name %q", err, name)
			}
		})
	}
}

func TestStoreDirectoryHasOneOwner(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	first, err := cache.Open(ctx, dir)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })
	second, err := cache.Open(ctx, dir)
	if err == nil {
		_ = second.Close()
		t.Fatal("second open succeeded; want ownership error")
	}
	if !strings.Contains(err.Error(), dir) {
		t.Fatalf("second open error = %v; want directory", err)
	}
	if !strings.Contains(err.Error(), "already open") {
		t.Fatalf("second open error = %v; want lock contention classification", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	reopened, err := cache.Open(ctx, dir)
	if err != nil {
		t.Fatalf("open after ownership release: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
}
