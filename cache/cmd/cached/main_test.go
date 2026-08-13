package main

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	cache "github.com/jonnyasmith/go/cache"
)

func TestHelpExitsSuccessfully(t *testing.T) {
	for _, mode := range []string{"repl", "load"} {
		t.Run(mode, func(t *testing.T) {
			var output, diagnostics bytes.Buffer
			err := run(context.Background(), []string{mode, "-h"}, strings.NewReader(""), &output, &diagnostics)
			if err != nil {
				t.Fatalf("%s help: %v", mode, err)
			}
			if !strings.Contains(diagnostics.String(), "Usage of "+mode+":") {
				t.Fatalf("%s help output = %q", mode, diagnostics.String())
			}
			if strings.Contains(diagnostics.String(), "flag: help requested") {
				t.Fatalf("%s help contains flag error: %q", mode, diagnostics.String())
			}
		})
	}
}

func TestREPLGrammarRejectsMalformedAndWhitespaceValues(t *testing.T) {
	store, err := cache.Open(context.Background(), t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	for _, test := range []struct {
		name string
		line string
		want string
	}{
		{name: "malformed get", line: "get key extra", want: "usage: get KEY"},
		{name: "whitespace value", line: "set key two words", want: "parse TTL"},
		{name: "misplaced TTL", line: "set key 1h value", want: "parse TTL"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			if _, err := executeREPL(store, test.line, &output); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("execute %q error = %v; want %q", test.line, err, test.want)
			}
		})
	}

	var output bytes.Buffer
	if _, err := executeREPL(store, "set key value 1h", &output); err != nil {
		t.Fatalf("valid TTL placement: %v", err)
	}
}

func TestREPLDrivesStoreCommands(t *testing.T) {
	binary := buildCached(t)
	dir := t.TempDir()
	commands := strings.Join([]string{
		"set plain value",
		"get plain",
		"set expiring temporary 1h",
		"delete plain",
		"get plain",
		"stats",
		"sync",
		"quit",
	}, "\n") + "\n"
	command := exec.Command(binary, "repl", "-dir", dir)
	command.Stdin = strings.NewReader(commands)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("run repl: %v\n%s", err, stderr.String())
	}
	for _, want := range []string{"value\n", "(nil)\n", "entries=1 ", "cached: store closed cleanly"} {
		if !strings.Contains(stdout.String()+stderr.String(), want) {
			t.Fatalf("repl output missing %q:\nstdout:\n%s\nstderr:\n%s", want, stdout.String(), stderr.String())
		}
	}

	store, err := cache.Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store.Close()
	value, ok := store.Get("expiring")
	if !ok || string(value) != "temporary" {
		t.Fatalf("TTL value after repl close = %q, %v", value, ok)
	}
	if _, ok := store.Get("plain"); ok {
		t.Fatal("deleted value reappeared after repl close")
	}
}

func TestLoadReadOnlyMixPerformsReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var stdout, stderr bytes.Buffer
	if err := runLoad(ctx, []string{"-dir", t.TempDir(), "-read-percent", "100", "-workers", "2", "-keyspace", "32"}, &stdout, &stderr); err != nil {
		t.Fatalf("run read-only load: %v\n%s", err, stderr.String())
	}
	if !regexp.MustCompile(`reads=[1-9][0-9]* writes=0`).MatchString(stderr.String()) {
		t.Fatalf("read-only load report = %q; want reads and no writes", stderr.String())
	}
}

func TestLoadMixClosesCleanlyOnSignals(t *testing.T) {
	for name, signal := range map[string]os.Signal{
		"interrupt":   os.Interrupt,
		"termination": syscall.SIGTERM,
	} {
		t.Run(name, func(t *testing.T) {
			binary := buildCached(t)
			dir := t.TempDir()
			command := exec.Command(binary, "load", "-dir", dir, "-read-percent", "50", "-workers", "2", "-keyspace", "32", "-ttl", "1h")
			stdout, err := command.StdoutPipe()
			if err != nil {
				t.Fatalf("stdout pipe: %v", err)
			}
			var stderr bytes.Buffer
			command.Stderr = &stderr
			if err := command.Start(); err != nil {
				t.Fatalf("start load: %v", err)
			}

			firstKey := make(chan string, 1)
			go func() {
				scanner := bufio.NewScanner(stdout)
				if scanner.Scan() {
					firstKey <- scanner.Text()
				}
				close(firstKey)
			}()
			var key string
			select {
			case key = <-firstKey:
				if key == "" {
					t.Fatalf("load exited before acknowledging a write: %s", stderr.String())
				}
			case <-time.After(10 * time.Second):
				_ = command.Process.Kill()
				_ = command.Wait()
				t.Fatal("load did not acknowledge a write")
			}
			if err := command.Process.Signal(signal); err != nil {
				t.Fatalf("signal load: %v", err)
			}
			if err := command.Wait(); err != nil {
				t.Fatalf("load signal shutdown: %v\n%s", err, stderr.String())
			}
			if !strings.Contains(stderr.String(), "cached: store closed cleanly") {
				t.Fatalf("shutdown report missing: %s", stderr.String())
			}

			store, err := cache.Open(context.Background(), dir)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer store.Close()
			if _, ok := store.Get(key); !ok {
				t.Fatalf("acknowledged key %q missing after signal shutdown", key)
			}
		})
	}
}

func buildCached(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "cached")
	command := exec.Command("go", "build", "-o", binary, ".")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build cached: %v\n%s", err, output)
	}
	return binary
}
