package cache_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
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
	chargedBytes := store.Bytes()
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
	if store.Len() != 1 || store.Bytes() != chargedBytes {
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

func TestReadsAfterCloseUseDetachedStaleView(t *testing.T) {
	dir := t.TempDir()
	store, err := cache.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := store.Set("key", []byte("before-close")); err != nil {
		t.Fatalf("set: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	value, ok := store.Get("key")
	if !ok || string(value) != "before-close" {
		t.Fatalf("get after close = %q, %v", value, ok)
	}
	dst, ok := store.GetInto("key", make([]byte, 0, len(value)))
	if !ok || string(dst) != "before-close" {
		t.Fatalf("get into after close = %q, %v", dst, ok)
	}
	if store.Len() != 1 || store.Bytes() == 0 {
		t.Fatalf("size after close = %d entries, %d bytes", store.Len(), store.Bytes())
	}
	if stats := store.Stats(); stats.Hits != 2 {
		t.Fatalf("stats after close = %+v; want two hits", stats)
	}

	reopened, err := cache.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("reopen while reading closed view: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if err := reopened.Set("key", []byte("after-reopen")); err != nil {
		t.Fatalf("set reopened: %v", err)
	}
	value, ok = store.Get("key")
	if !ok || string(value) != "before-close" {
		t.Fatalf("closed view after reopened write = %q, %v; want stale value", value, ok)
	}
}

func TestClosePersistsEveryConcurrentWriteItAccepts(t *testing.T) {
	dir := t.TempDir()
	store, err := cache.Open(context.Background(), dir, cache.WithFlushInterval(time.Hour))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	type result struct {
		key string
		err error
	}
	const writers = 256
	start := make(chan struct{})
	results := make(chan result, writers)
	var group sync.WaitGroup
	for index := range writers {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			<-start
			key := fmt.Sprintf("key-%03d", index)
			results <- result{key: key, err: store.Set(key, []byte(key))}
		}(index)
	}
	close(start)
	first := <-results
	closeResult := make(chan error, 1)
	go func() { closeResult <- store.Close() }()
	group.Wait()
	close(results)
	if err := <-closeResult; err != nil {
		t.Fatalf("close: %v", err)
	}

	accepted := make([]string, 0, writers)
	if first.err != nil {
		t.Fatalf("first set before close: %v", first.err)
	}
	accepted = append(accepted, first.key)
	for result := range results {
		switch {
		case result.err == nil:
			accepted = append(accepted, result.key)
		case errors.Is(result.err, cache.ErrClosed):
		default:
			t.Fatalf("set %q during close: %v", result.key, result.err)
		}
	}

	reopened, err := cache.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	for _, key := range accepted {
		value, ok := reopened.Get(key)
		if !ok || string(value) != key {
			t.Fatalf("accepted key %q recovered as %q, %v", key, value, ok)
		}
	}
}

func TestOpenRejectsInvalidOptions(t *testing.T) {
	ctx := context.Background()
	for name, option := range map[string]cache.Option{
		"WithShards":        cache.WithShards(3),
		"WithCapacity":      cache.WithCapacity(63),
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
