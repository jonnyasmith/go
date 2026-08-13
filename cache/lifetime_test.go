package cache_test

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	cache "github.com/jonnyasmith/go/cache"
)

const smallEntryBytes = uint64(66) // one-byte key, one-byte value, and the documented 64-byte overhead

func TestSetTTLPersistsAbsoluteDeadlineAndRecoveryExpiresIt(t *testing.T) {
	dir := t.TempDir()
	store, err := cache.Open(context.Background(), dir, cache.WithShards(1), cache.WithSweepInterval(time.Hour))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	before := time.Now()
	if err := store.SetTTL("k", []byte("v"), 40*time.Millisecond); err != nil {
		t.Fatalf("set ttl: %v", err)
	}
	after := time.Now()
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	segment, err := os.ReadFile(filepath.Join(dir, "00000001.seg"))
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	deadline := int64(binary.LittleEndian.Uint64(segment[19:27]))
	if deadline < before.Add(40*time.Millisecond).UnixNano() || deadline > after.Add(40*time.Millisecond).UnixNano() {
		t.Fatalf("record deadline = %d; want absolute accept-time deadline between %d and %d", deadline, before.Add(40*time.Millisecond).UnixNano(), after.Add(40*time.Millisecond).UnixNano())
	}

	time.Sleep(time.Until(time.Unix(0, deadline)) + time.Millisecond)
	store, err = cache.Open(context.Background(), dir, cache.WithShards(1), cache.WithSweepInterval(time.Hour))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, ok := store.Get("k"); ok || store.Len() != 0 {
		t.Fatalf("expired recovered entry = present %v, len %d; want absent, 0", ok, store.Len())
	}
}

func TestSetTTLRejectsNonPositiveLifetime(t *testing.T) {
	store, err := cache.Open(context.Background(), t.TempDir(), cache.WithShards(1))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	for _, ttl := range []time.Duration{0, -time.Second} {
		if err := store.SetTTL("k", []byte("v"), ttl); err == nil {
			t.Fatalf("SetTTL with %s succeeded; want error", ttl)
		}
	}
	if store.Len() != 0 || store.Stats().RecordsWritten != 0 {
		t.Fatalf("rejected TTLs changed store: len %d, stats %+v", store.Len(), store.Stats())
	}
}

func TestDeadlineControlsVisibilityBeforeReclamation(t *testing.T) {
	store, err := cache.Open(context.Background(), t.TempDir(), cache.WithShards(1), cache.WithSweepInterval(time.Hour))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.Set("permanent", []byte("v")); err != nil {
		t.Fatalf("set permanent: %v", err)
	}
	if err := store.SetTTL("short", []byte("v"), 10*time.Millisecond); err != nil {
		t.Fatalf("set ttl: %v", err)
	}
	time.Sleep(15 * time.Millisecond)
	if _, ok := store.Get("short"); ok {
		t.Fatal("expired entry is observable")
	}
	if got, ok := store.Get("permanent"); !ok || string(got) != "v" {
		t.Fatalf("permanent entry = %q, %v; want v, true", got, ok)
	}
	stats := store.Stats()
	if stats.Expiries != 1 || stats.Misses != 1 || stats.RecordsWritten != 2 {
		t.Fatalf("stats = %+v; want one expiry, one miss, and only the two caller records", stats)
	}
}

func TestSweepReclaimsExpiredEntries(t *testing.T) {
	store, err := cache.Open(context.Background(), t.TempDir(), cache.WithShards(4), cache.WithSweepInterval(8*time.Millisecond))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	for _, key := range []string{"a", "b", "c", "d"} {
		if err := store.SetTTL(key, []byte("v"), 5*time.Millisecond); err != nil {
			t.Fatalf("set ttl %q: %v", key, err)
		}
	}
	deadline := time.Now().Add(250 * time.Millisecond)
	for store.Bytes() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if store.Bytes() != 0 || store.Len() != 0 {
		t.Fatalf("swept size = %d entries, %d bytes; want zero", store.Len(), store.Bytes())
	}
	if stats := store.Stats(); stats.Expiries != 4 || stats.RecordsWritten != 4 {
		t.Fatalf("stats = %+v; want four expiries and no derived records", stats)
	}
}

func TestCapacityEvictsShardLRUAndRecoveryReplaysIt(t *testing.T) {
	dir := t.TempDir()
	store, err := cache.Open(context.Background(), dir, cache.WithShards(1), cache.WithCapacity(2*smallEntryBytes))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, key := range []string{"a", "b"} {
		if err := store.Set(key, []byte("v")); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
	}
	if _, ok := store.Get("a"); !ok {
		t.Fatal("touch a: miss")
	}
	if err := store.Set("c", []byte("v")); err != nil {
		t.Fatalf("set c: %v", err)
	}
	if _, ok := store.Get("b"); ok {
		t.Fatal("least recently used b remains present")
	}
	if _, ok := store.Get("a"); !ok {
		t.Fatal("recently used a was evicted")
	}
	if store.Len() != 2 || store.Bytes() != 2*smallEntryBytes {
		t.Fatalf("bounded size = %d entries, %d bytes", store.Len(), store.Bytes())
	}
	if stats := store.Stats(); stats.Evictions != 1 || stats.RecordsWritten != 3 {
		t.Fatalf("stats = %+v; want one eviction and only three caller records", stats)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	store, err = cache.Open(context.Background(), dir, cache.WithShards(1), cache.WithCapacity(3*smallEntryBytes))
	if err != nil {
		t.Fatalf("reopen larger: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, key := range []string{"a", "b", "c"} {
		if _, ok := store.Get(key); !ok {
			t.Fatalf("evicted key %q did not reappear after recovery", key)
		}
	}
}

func TestRecoveryHonoursSmallerCapacity(t *testing.T) {
	dir := t.TempDir()
	store, err := cache.Open(context.Background(), dir, cache.WithShards(1), cache.WithCapacity(3*smallEntryBytes))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, key := range []string{"a", "b", "c"} {
		if err := store.Set(key, []byte("v")); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	store, err = cache.Open(context.Background(), dir, cache.WithShards(1), cache.WithCapacity(2*smallEntryBytes))
	if err != nil {
		t.Fatalf("reopen smaller: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if store.Len() != 2 || store.Bytes() > 2*smallEntryBytes {
		t.Fatalf("recovered size = %d entries, %d bytes; want two entries within capacity", store.Len(), store.Bytes())
	}
	if stats := store.Stats(); stats.Evictions != 1 {
		t.Fatalf("recovery stats = %+v; want one eviction", stats)
	}
}
