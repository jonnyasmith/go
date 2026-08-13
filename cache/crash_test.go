package cache_test

import (
	"bufio"
	"bytes"
	"context"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	cache "github.com/jonnyasmith/go/cache"
)

func TestKilledLoadProcessRecoversEveryAcknowledgedWrite(t *testing.T) {
	dir, acknowledged := killLoad(t, nil, func(_ string, count int) bool { return count >= 100 })
	assertAcknowledgedPresent(t, dir, acknowledged, 0)
}

func TestKilledLoadProcessRecoversEntriesWithTTLs(t *testing.T) {
	dir, acknowledged := killLoad(t, []string{"-ttl", "1h"}, func(_ string, count int) bool { return count >= 100 })
	assertAcknowledgedPresent(t, dir, acknowledged, 0)
}

func TestKilledLoadProcessRecoversUnderCapacityPressure(t *testing.T) {
	const capacity = uint64(4096)
	dir, _ := killLoad(t, []string{"-capacity", "4096", "-shards", "1"}, func(_ string, count int) bool { return count >= 100 })
	store, err := cache.Open(context.Background(), dir, cache.WithCapacity(capacity), cache.WithShards(1))
	if err != nil {
		t.Fatalf("reopen after capacity-pressure kill: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if store.Len() == 0 || store.Bytes() > capacity {
		t.Fatalf("recovered bounded store = %d entries, %d bytes", store.Len(), store.Bytes())
	}
	if store.Stats().Evictions == 0 {
		t.Fatal("recovery under capacity pressure reported no evictions")
	}
}

func TestKilledDuringSnapshotInstallRecoversAcknowledgedWrites(t *testing.T) {
	dir, acknowledged := killLoad(t,
		[]string{"-snapshot-threshold", "16777216", "-value-bytes", "262144"},
		func(dir string, count int) bool {
			if count < 64 {
				return false
			}
			temporary, _ := filepath.Glob(filepath.Join(dir, ".snapshot-*"))
			return len(temporary) != 0
		},
	)
	assertAcknowledgedPresent(t, dir, acknowledged, 262144)
}

func killLoad(t *testing.T, extraArgs []string, ready func(dir string, acknowledged int) bool) (string, []string) {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "cached")
	build := exec.Command("go", "build", "-o", binary, "./cmd/cached")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build cached: %v\n%s", err, output)
	}

	dir := t.TempDir()
	args := append([]string{"load", "-dir", dir}, extraArgs...)
	command := exec.Command(binary, args...)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatalf("start cached: %v", err)
	}

	var mu sync.Mutex
	acknowledged := make([]string, 0, 128)
	scanned := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			mu.Lock()
			acknowledged = append(acknowledged, scanner.Text())
			mu.Unlock()
		}
		scanned <- scanner.Err()
	}()

	deadline := time.NewTimer(15 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	readyToKill := false
	for !readyToKill {
		select {
		case <-ticker.C:
			mu.Lock()
			count := len(acknowledged)
			mu.Unlock()
			readyToKill = ready(dir, count)
		case <-deadline.C:
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatalf("load process did not reach crash point: %s", stderr.String())
		}
	}

	if err := command.Process.Kill(); err != nil {
		t.Fatalf("kill cached: %v", err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("killed process exited successfully")
	}
	if err := <-scanned; err != nil {
		t.Fatalf("scan acknowledgements: %v", err)
	}
	mu.Lock()
	result := append([]string(nil), acknowledged...)
	mu.Unlock()
	return dir, result
}

func assertAcknowledgedPresent(t *testing.T, dir string, acknowledged []string, valueBytes int) {
	t.Helper()
	store, err := cache.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("reopen after kill: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for _, key := range acknowledged {
		want := []byte(key)
		if valueBytes != 0 {
			want = make([]byte, valueBytes)
			copy(want, key)
		}
		value, ok := store.Get(key)
		if !ok || !bytes.Equal(value, want) {
			t.Fatalf("acknowledged key %q recovered as %d bytes, present %v", key, len(value), ok)
		}
	}
}
