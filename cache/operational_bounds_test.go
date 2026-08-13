package cache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestOversizedEntriesAreRejectedBeforeSubmission(t *testing.T) {
	const (
		shards        = 4
		shardCapacity = uint64(80)
	)
	store, err := Open(context.Background(), t.TempDir(), WithShards(shards), WithCapacity(shards*shardCapacity))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	if err := store.Set("k", []byte(strings.Repeat("v", 15))); err != nil {
		t.Fatalf("set exactly fitting entry: %v", err)
	}
	before := store.Stats()
	beforeBytes := store.Bytes()
	beforeLen := store.Len()

	for name, set := range map[string]func() error{
		"Set":    func() error { return store.Set("x", []byte(strings.Repeat("v", 16))) },
		"SetTTL": func() error { return store.SetTTL("x", []byte(strings.Repeat("v", 16)), time.Minute) },
	} {
		t.Run(name, func(t *testing.T) {
			if err := set(); err == nil || !strings.Contains(err.Error(), "target shard capacity") {
				t.Fatalf("oversized entry error = %v; want target shard capacity error", err)
			}
			if got := store.Stats(); got != before {
				t.Fatalf("stats after rejection = %+v; want %+v", got, before)
			}
			if store.Bytes() != beforeBytes || store.Len() != beforeLen {
				t.Fatalf("size after rejection = %d entries, %d bytes; want %d, %d", store.Len(), store.Bytes(), beforeLen, beforeBytes)
			}
		})
	}
}

func TestEveryCloseCallerReceivesTerminalResult(t *testing.T) {
	for _, test := range []struct {
		name      string
		failClose bool
	}{
		{name: "success"},
		{name: "failure", failClose: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			store, err := Open(context.Background(), dir)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if err := store.Set("accepted", []byte("value")); err != nil {
				t.Fatalf("set: %v", err)
			}

			terminalFailure := errors.New("injected final snapshot sync failure")
			if test.failClose {
				store.directorySync = func(string) error { return terminalFailure }
			}

			const callers = 32
			start := make(chan struct{})
			results := make(chan error, callers)
			var group sync.WaitGroup
			for range callers {
				group.Add(1)
				go func() {
					defer group.Done()
					<-start
					results <- store.Close()
				}()
			}
			close(start)
			group.Wait()
			close(results)

			var first string
			for result := range results {
				if test.failClose != errors.Is(result, terminalFailure) {
					t.Fatalf("close result = %v; failure expected %v", result, test.failClose)
				}
				if first == "" && result != nil {
					first = result.Error()
				} else if result != nil && result.Error() != first {
					t.Fatalf("close result = %q; want stable %q", result, first)
				}
			}
			repeated := store.Close()
			if test.failClose != errors.Is(repeated, terminalFailure) {
				t.Fatalf("repeated close = %v; failure expected %v", repeated, test.failClose)
			}
			if repeated != nil && repeated.Error() != first {
				t.Fatalf("repeated close = %q; want %q", repeated, first)
			}

			reopened, err := Open(context.Background(), dir)
			if err != nil {
				t.Fatalf("open after every close returned: %v", err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			if value, ok := reopened.Get("accepted"); !ok || string(value) != "value" {
				t.Fatalf("accepted write after reopen = %q, %v", value, ok)
			}
		})
	}
}

func TestAutomaticCompactionAfterEvictionRetainsDurableImage(t *testing.T) {
	const entries = 24
	entryBytes := entryOverhead + uint64(len("00")+len("value"))
	dir := t.TempDir()
	store, err := Open(
		context.Background(),
		dir,
		WithShards(1),
		WithCapacity(2*entryBytes),
		WithSnapshotThreshold(1),
		WithFlushInterval(time.Hour),
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	previousSnapshots := uint64(0)
	compactionBaseline := uint64(0)
	for index := range entries {
		key := string([]byte{'a' + byte(index/10), '0' + byte(index%10)})
		if err := store.Set(key, []byte("value")); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
		time.Sleep(5 * time.Millisecond)
		if index == 2 {
			for store.snapshotRunning.Load() {
				time.Sleep(time.Millisecond)
			}
			compactionBaseline = store.Stats().Snapshots
			previousSnapshots = compactionBaseline
		}
		currentSnapshots := store.Stats().Snapshots
		if currentSnapshots > previousSnapshots {
			previousSnapshots = currentSnapshots
			segments, globErr := filepath.Glob(filepath.Join(dir, "*.seg"))
			if globErr != nil {
				t.Fatalf("list segments: %v", globErr)
			}
			if len(segments) > 2 {
				t.Fatalf("retained segments after compaction = %d; want at most 2", len(segments))
			}
		}
	}
	if stats := store.Stats(); stats.Evictions == 0 || previousSnapshots < compactionBaseline+2 {
		t.Fatalf("stats = %+v; want evictions and two compactions after baseline %d", stats, compactionBaseline)
	}
	if err := store.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	crashImage := t.TempDir()
	copyDurableFiles(t, dir, crashImage)
	crashRecovered, err := Open(context.Background(), crashImage, WithShards(1), WithCapacity(entries*entryBytes))
	if err != nil {
		t.Fatalf("open crash image: %v", err)
	}
	for index := range entries {
		key := string([]byte{'a' + byte(index/10), '0' + byte(index%10)})
		if value, ok := crashRecovered.Get(key); !ok || string(value) != "value" {
			t.Fatalf("crash-recovered %q = %q, %v", key, value, ok)
		}
	}
	if err := crashRecovered.Close(); err != nil {
		t.Fatalf("close crash image: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
func TestConcurrentWritesSurviveDurableCompaction(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(
		context.Background(),
		dir,
		WithShards(1),
		WithCapacity(160),
		WithSnapshotThreshold(1),
		WithFlushInterval(time.Hour),
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, key := range []string{"seed-a", "seed-b", "seed-c"} {
		if err := store.Set(key, []byte("value")); err != nil {
			t.Fatalf("seed %q: %v", key, err)
		}
	}
	for store.snapshotRunning.Load() {
		time.Sleep(time.Millisecond)
	}
	if err := store.Set("trigger", []byte("value")); err != nil {
		t.Fatalf("trigger compaction: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !store.snapshotRunning.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !store.snapshotRunning.Load() {
		t.Fatal("automatic compaction did not start")
	}

	const writes = 128
	var group sync.WaitGroup
	for index := range writes {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			key := "concurrent-" + string(rune(index+0x100))
			if err := store.Set(key, []byte("value")); err != nil {
				t.Errorf("set %q: %v", key, err)
			}
		}(index)
	}
	group.Wait()
	for store.snapshotRunning.Load() {
		time.Sleep(time.Millisecond)
	}
	if err := store.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	crashImage := t.TempDir()
	copyDurableFiles(t, dir, crashImage)
	recovered, err := Open(context.Background(), crashImage, WithShards(1), WithCapacity(1<<20))
	if err != nil {
		t.Fatalf("open crash image: %v", err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	for index := range writes {
		key := "concurrent-" + string(rune(index+0x100))
		if value, ok := recovered.Get(key); !ok || string(value) != "value" {
			t.Fatalf("recovered %q = %q, %v", key, value, ok)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func copyDurableFiles(t *testing.T, source, target string) {
	t.Helper()
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatalf("read durable source: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == "LOCK" || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		contents, readErr := os.ReadFile(filepath.Join(source, entry.Name()))
		if readErr != nil {
			t.Fatalf("read %s: %v", entry.Name(), readErr)
		}
		if writeErr := os.WriteFile(filepath.Join(target, entry.Name()), contents, 0o600); writeErr != nil {
			t.Fatalf("copy %s: %v", entry.Name(), writeErr)
		}
	}
}
