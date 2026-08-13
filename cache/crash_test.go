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
	binary := filepath.Join(t.TempDir(), "cached")
	build := exec.Command("go", "build", "-o", binary, "./cmd/cached")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build cached: %v\n%s", err, output)
	}

	dir := t.TempDir()
	command := exec.Command(binary, "load", "-dir", dir)
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
	reached := make(chan struct{})
	scanned := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		notified := false
		for scanner.Scan() {
			mu.Lock()
			acknowledged = append(acknowledged, scanner.Text())
			count := len(acknowledged)
			mu.Unlock()
			if count >= 100 && !notified {
				close(reached)
				notified = true
			}
		}
		scanned <- scanner.Err()
	}()

	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatalf("load process did not acknowledge writes: %s", stderr.String())
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

	store, err := cache.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("reopen after kill: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	mu.Lock()
	defer mu.Unlock()
	for _, key := range acknowledged {
		value, ok := store.Get(key)
		if !ok || string(value) != key {
			t.Fatalf("acknowledged key %q recovered as %q, %v", key, value, ok)
		}
	}
}
