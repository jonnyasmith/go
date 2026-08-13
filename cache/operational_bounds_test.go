package cache

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
)

func TestOversizedEntriesAreRejectedBeforeSubmission(t *testing.T) {
	for _, shardCapacity := range []uint64{80, 257} {
		t.Run(fmt.Sprintf("capacity-%d", shardCapacity), func(t *testing.T) {
			const shards = 4
			dir := t.TempDir()
			store, err := Open(context.Background(), dir, WithShards(shards), WithCapacity(shards*shardCapacity))
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			t.Cleanup(func() { _ = store.Close() })

			exactValueLength := int(shardCapacity - entryOverhead - 1)
			if err := store.Set("k", []byte(strings.Repeat("v", exactValueLength))); err != nil {
				t.Fatalf("set exactly fitting entry: %v", err)
			}
			before := store.Stats()
			beforeBytes := store.Bytes()
			beforeLen := store.Len()
			segment := filepath.Join(dir, segmentName(1))
			beforeInfo, err := os.Stat(segment)
			if err != nil {
				t.Fatalf("stat WAL before rejection: %v", err)
			}

			for name, set := range map[string]func() error{
				"Set":    func() error { return store.Set("x", []byte(strings.Repeat("v", exactValueLength+1))) },
				"SetTTL": func() error { return store.SetTTL("x", []byte(strings.Repeat("v", exactValueLength+1)), time.Minute) },
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
					afterInfo, statErr := os.Stat(segment)
					if statErr != nil {
						t.Fatalf("stat WAL after rejection: %v", statErr)
					}
					if afterInfo.Size() != beforeInfo.Size() {
						t.Fatalf("WAL size after rejection = %d; want %d", afterInfo.Size(), beforeInfo.Size())
					}
				})
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

func TestAutomaticSnapshotSuccessClearsEarlierError(t *testing.T) {
	store, err := Open(context.Background(), t.TempDir(), WithShards(1))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	injected := errors.New("injected snapshot creation failure")
	originalFiles := store.files.Load()
	failingFiles := *originalFiles
	failingFiles.createTemp = func(string, string) (*os.File, error) { return nil, injected }
	store.files.Store(&failingFiles)
	if !store.startSnapshot(&logState{seq: store.logSequence.Load()}) {
		t.Fatal("start failing automatic snapshot: false")
	}
	waitForSnapshot(t, store)
	if got := store.Stats().LastError; !strings.Contains(got, injected.Error()) {
		t.Fatalf("last error after failure = %q; want %q", got, injected)
	}

	store.files.Store(originalFiles)
	store.shards[0].mu.Lock()
	if !store.startSnapshot(&logState{seq: store.logSequence.Load()}) {
		store.shards[0].mu.Unlock()
		t.Fatal("start discarded automatic snapshot: false")
	}
	store.evictionInterlock.Lock()
	store.evictionGeneration++
	store.evictionInterlock.Unlock()
	store.shards[0].mu.Unlock()
	waitForSnapshot(t, store)
	if got := store.Stats().LastError; !strings.Contains(got, injected.Error()) {
		t.Fatalf("last error after discarded snapshot = %q; want prior error %q", got, injected)
	}
	store.evictionInterlock.Lock()
	store.evictionGeneration = 0
	store.evictionInterlock.Unlock()

	if !store.startSnapshot(&logState{seq: store.logSequence.Load()}) {
		t.Fatal("start successful automatic snapshot: false")
	}
	waitForSnapshot(t, store)
	stats := store.Stats()
	if stats.LastError != "" || stats.Snapshots != 1 {
		t.Fatalf("stats after successful retry = %+v; want cleared error and one completed snapshot", stats)
	}
}

func TestSnapshotCounterUsesCompletedInstallationAndCleanupBoundary(t *testing.T) {
	t.Run("directory sync after rename", func(t *testing.T) {
		store, err := Open(context.Background(), t.TempDir(), WithShards(1))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })
		if err := store.Set("key", []byte("value")); err != nil {
			t.Fatalf("set: %v", err)
		}
		injected := errors.New("injected directory sync failure")
		store.directorySync = func(string) error { return injected }
		err = store.writeSnapshot()
		store.directorySync = syncDirectory
		if err == nil || !strings.Contains(err.Error(), "is installed but directory sync failed") {
			t.Fatalf("snapshot error = %v; want installed directory-sync failure", err)
		}
		if got := store.Stats().Snapshots; got != 0 {
			t.Fatalf("snapshots = %d; want zero before completed cleanup", got)
		}
	})

	t.Run("cleanup after installation", func(t *testing.T) {
		store, err := Open(context.Background(), t.TempDir(), WithShards(1))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })
		if err := store.Set("key", []byte("value")); err != nil {
			t.Fatalf("set: %v", err)
		}
		stale := filepath.Join(store.dir, snapshotName(0))
		if err := os.WriteFile(stale, []byte("stale"), 0o600); err != nil {
			t.Fatalf("write stale snapshot: %v", err)
		}
		injected := errors.New("injected cleanup failure")
		originalFiles := store.files.Load()
		failingFiles := *originalFiles
		failingFiles.remove = func(path string) error {
			if path == stale {
				return injected
			}
			return os.Remove(path)
		}
		store.files.Store(&failingFiles)
		err = store.writeSnapshot()
		store.files.Store(originalFiles)
		if err == nil || !strings.Contains(err.Error(), "is installed but cleanup failed") || !strings.Contains(err.Error(), stale) {
			t.Fatalf("snapshot error = %v; want installed cleanup failure naming %q", err, stale)
		}
		if got := store.Stats().Snapshots; got != 0 {
			t.Fatalf("snapshots = %d; want zero before completed cleanup", got)
		}
	})
}

func TestOpenRemovesOnlyOwnedTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(context.Background(), dir, WithShards(1))
	if err != nil {
		t.Fatalf("open initial store: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close initial store: %v", err)
	}
	installedSegments, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil {
		t.Fatalf("list installed segments: %v", err)
	}
	installedSnapshots, err := filepath.Glob(filepath.Join(dir, "*.snap"))
	if err != nil {
		t.Fatalf("list installed snapshots: %v", err)
	}

	temporarySegment := filepath.Join(dir, ".segment-interrupted")
	temporarySnapshot := filepath.Join(dir, ".snapshot-interrupted")
	unrelated := filepath.Join(dir, ".unrelated")
	temporaryDirectory := filepath.Join(dir, ".segment-directory")
	outside := filepath.Join(t.TempDir(), ".snapshot-outside")
	for _, path := range []string{temporarySegment, temporarySnapshot, unrelated, outside} {
		if err := os.WriteFile(path, []byte("keep-or-remove"), 0o600); err != nil {
			t.Fatalf("write %q: %v", path, err)
		}
	}
	if err := os.Mkdir(temporaryDirectory, 0o700); err != nil {
		t.Fatalf("make temporary-looking directory: %v", err)
	}

	store, err = Open(context.Background(), dir, WithShards(1))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, path := range []string{temporarySegment, temporarySnapshot} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("temporary file %q still exists: %v", path, err)
		}
	}
	for _, path := range append(append(installedSegments, installedSnapshots...), filepath.Join(dir, "LOCK"), unrelated, temporaryDirectory, outside) {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("preserved path %q: %v", path, err)
		}
	}
}

func TestTemporaryCleanupErrorNamesPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".segment-interrupted")
	if err := os.WriteFile(path, []byte("partial"), 0o600); err != nil {
		t.Fatalf("write temporary segment: %v", err)
	}
	store := newStoreState(dir, nil, 1, 1<<20, nil)
	injected := errors.New("injected cleanup failure")
	failingFiles := *store.files.Load()
	failingFiles.remove = func(string) error { return injected }
	store.files.Store(&failingFiles)
	err := store.removeTemporaryFiles()
	if err == nil || !strings.Contains(err.Error(), path) || !errors.Is(err, injected) {
		t.Fatalf("cleanup error = %v; want path %q and injected cause", err, path)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("temporary file after failed cleanup: %v", err)
	}
}

func TestWALFailuresLatchThroughPerStoreFilesystemSeam(t *testing.T) {
	for _, failurePoint := range []string{"create", "write", "sync"} {
		t.Run(failurePoint, func(t *testing.T) {
			dir := t.TempDir()
			store, err := Open(context.Background(), dir, WithShards(1), WithSegmentSize(9))
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if err := store.Set("live", []byte("value")); err != nil {
				t.Fatalf("seed live value: %v", err)
			}

			injected := fmt.Errorf("injected WAL %s failure", failurePoint)
			originalFiles := store.files.Load()
			failingFiles := *originalFiles
			switch failurePoint {
			case "create":
				failingFiles.createTemp = func(string, string) (*os.File, error) { return nil, injected }
			case "write":
				failingFiles.writeWAL = func(*os.File, []byte) (int, error) { return 0, injected }
			case "sync":
				failingFiles.syncWAL = func(*os.File) error { return injected }
			}
			store.files.Store(&failingFiles)
			var firstErr error
			if failurePoint == "sync" {
				firstErr = store.Sync()
			} else {
				firstErr = store.Set("rejected", []byte("value"))
			}
			if firstErr == nil || !errors.Is(firstErr, injected) {
				t.Fatalf("first failure = %v; want injected cause", firstErr)
			}
			if secondErr := store.Delete("live"); secondErr != firstErr {
				t.Fatalf("latched error = %v; want same error value %v", secondErr, firstErr)
			}
			if value, ok := store.Get("live"); !ok || string(value) != "value" {
				t.Fatalf("read after failure = %q, %v; want value, true", value, ok)
			}
			if got := store.Stats().LastError; !strings.Contains(got, injected.Error()) {
				t.Fatalf("last error = %q; want %q", got, injected)
			}

			store.files.Store(originalFiles)
			if err := store.Close(); err == nil {
				t.Fatal("close after latched durability failure unexpectedly succeeded")
			}

			reopened, err := Open(context.Background(), dir, WithShards(1))
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			if err := reopened.Set("new", []byte("value")); err != nil {
				t.Fatalf("set after reopen: %v", err)
			}
		})
	}
}

func waitForSnapshot(t *testing.T, store *Store) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for store.snapshotRunning.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if store.snapshotRunning.Load() {
		t.Fatal("snapshot did not finish")
	}
}
func TestFinalSnapshotKeepsEvictedLiveEntriesOnly(t *testing.T) {
	dir := t.TempDir()
	entryBytes := entryOverhead + uint64(len("resident")+len("v"))
	store, err := Open(context.Background(), dir, WithShards(1), WithCapacity(2*entryBytes), WithSweepInterval(time.Hour))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, key := range []string{"live-a", "live-b", "resident"} {
		if err := store.Set(key, []byte("v")); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
	}
	if err := store.Set("deleted", []byte("v")); err != nil {
		t.Fatalf("set deleted: %v", err)
	}
	if err := store.Delete("deleted"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := store.SetTTL("expired", []byte("v"), time.Millisecond); err != nil {
		t.Fatalf("set expiring: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if stats := store.Stats(); stats.Evictions == 0 {
		t.Fatalf("stats = %+v; want at least one eviction before close", stats)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(context.Background(), dir, WithShards(1), WithCapacity(1<<20))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	for _, key := range []string{"live-a", "live-b", "resident"} {
		if value, ok := reopened.Get(key); !ok || string(value) != "v" {
			t.Fatalf("recovered live key %q = %q, %v; want v, true", key, value, ok)
		}
	}
	for _, key := range []string{"deleted", "expired"} {
		if value, ok := reopened.Get(key); ok {
			t.Fatalf("recovered removed key %q = %q, true; want absent", key, value)
		}
	}
}

func BenchmarkFinalSnapshotAfterEviction(b *testing.B) {
	dir := b.TempDir()
	const entries = 256
	value := []byte(strings.Repeat("v", 256))
	entryBytes := entryOverhead + uint64(len("key-000")+len(value))
	store, err := Open(context.Background(), dir, WithShards(8), WithCapacity(8*8*entryBytes), WithSweepInterval(time.Hour))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	for index := range entries {
		if err := store.Set(fmt.Sprintf("key-%03d", index), value); err != nil {
			b.Fatalf("set %d: %v", index, err)
		}
	}
	if stats := store.Stats(); stats.Evictions == 0 {
		b.Fatalf("stats = %+v; benchmark requires eviction-aware reconstruction", stats)
	}
	b.Cleanup(func() { _ = store.Close() })
	b.ReportAllocs()
	for b.Loop() {
		if err := store.writeFinalSnapshot(store.logSequence.Load()); err != nil {
			b.Fatalf("write final snapshot: %v", err)
		}
	}
}
