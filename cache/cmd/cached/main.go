package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	cache "github.com/jonnyasmith/go/cache"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, input io.Reader, output, diagnostics io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: cached repl -dir DIRECTORY | cached load -dir DIRECTORY [options]")
	}
	switch args[0] {
	case "repl":
		return runREPL(ctx, args[1:], input, output, diagnostics)
	case "load":
		return runLoad(ctx, args[1:], output, diagnostics)
	default:
		return fmt.Errorf("cached: unknown mode %q; want repl or load", args[0])
	}
}

type storeFlags struct {
	dir               string
	capacity          uint64
	shards            int
	snapshotThreshold int64
}

func addStoreFlags(flags *flag.FlagSet) *storeFlags {
	config := new(storeFlags)
	flags.StringVar(&config.dir, "dir", "", "store directory")
	flags.Uint64Var(&config.capacity, "capacity", 0, "capacity in bytes (zero uses the default)")
	flags.IntVar(&config.shards, "shards", 0, "shard count (zero uses the default)")
	flags.Int64Var(&config.snapshotThreshold, "snapshot-threshold", 0, "log bytes between snapshots (zero uses the default)")
	return config
}

func (config *storeFlags) open(ctx context.Context) (*cache.Store, error) {
	if config.dir == "" {
		return nil, errors.New("-dir is required")
	}
	options := make([]cache.Option, 0, 3)
	if config.capacity != 0 {
		options = append(options, cache.WithCapacity(config.capacity))
	}
	if config.shards != 0 {
		options = append(options, cache.WithShards(config.shards))
	}
	if config.snapshotThreshold != 0 {
		options = append(options, cache.WithSnapshotThreshold(config.snapshotThreshold))
	}
	return cache.Open(ctx, config.dir, options...)
}

func closeStore(store *cache.Store, diagnostics io.Writer) error {
	if err := store.Close(); err != nil {
		return fmt.Errorf("close store: %w", err)
	}
	_, err := fmt.Fprintln(diagnostics, "cached: store closed cleanly")
	return err
}

func runREPL(ctx context.Context, args []string, input io.Reader, output, diagnostics io.Writer) error {
	flags := flag.NewFlagSet("repl", flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	config := addStoreFlags(flags)
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	store, err := config.open(ctx)
	if err != nil {
		return fmt.Errorf("cached repl: %w", err)
	}

	lines := make(chan string)
	scanResult := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(input)
		for scanner.Scan() {
			select {
			case lines <- scanner.Text():
			case <-ctx.Done():
				scanResult <- nil
				return
			}
		}
		scanResult <- scanner.Err()
	}()

	runErr := serveREPL(ctx, store, lines, scanResult, output, diagnostics)
	return errors.Join(runErr, closeStore(store, diagnostics))
}

func serveREPL(ctx context.Context, store *cache.Store, lines <-chan string, scanResult <-chan error, output, diagnostics io.Writer) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-scanResult:
			return err
		case line := <-lines:
			stop, err := executeREPL(store, line, output)
			if err != nil {
				if _, writeErr := fmt.Fprintf(diagnostics, "error: %v\n", err); writeErr != nil {
					return writeErr
				}
			}
			if stop {
				return nil
			}
		}
	}
}

func executeREPL(store *cache.Store, line string, output io.Writer) (bool, error) {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return false, nil
	}
	switch fields[0] {
	case "set":
		if len(fields) != 3 && len(fields) != 4 {
			return false, errors.New("usage: set KEY VALUE [TTL]")
		}
		var err error
		if len(fields) == 4 {
			ttl, parseErr := time.ParseDuration(fields[3])
			if parseErr != nil {
				return false, fmt.Errorf("parse TTL: %w", parseErr)
			}
			err = store.SetTTL(fields[1], []byte(fields[2]), ttl)
		} else {
			err = store.Set(fields[1], []byte(fields[2]))
		}
		if err != nil {
			return false, err
		}
		_, err = fmt.Fprintln(output, "OK")
		return false, err
	case "get":
		if len(fields) != 2 {
			return false, errors.New("usage: get KEY")
		}
		value, ok := store.Get(fields[1])
		if !ok {
			_, err := fmt.Fprintln(output, "(nil)")
			return false, err
		}
		_, err := fmt.Fprintln(output, string(value))
		return false, err
	case "delete":
		if len(fields) != 2 {
			return false, errors.New("usage: delete KEY")
		}
		if err := store.Delete(fields[1]); err != nil {
			return false, err
		}
		_, err := fmt.Fprintln(output, "OK")
		return false, err
	case "stats":
		if len(fields) != 1 {
			return false, errors.New("usage: stats")
		}
		stats := store.Stats()
		_, err := fmt.Fprintf(output, "entries=%d bytes=%d hits=%d misses=%d expiries=%d evictions=%d records=%d bytes_written=%d fsyncs=%d snapshots=%d last_error=%q\n",
			store.Len(), store.Bytes(), stats.Hits, stats.Misses, stats.Expiries, stats.Evictions,
			stats.RecordsWritten, stats.BytesWritten, stats.Fsyncs, stats.Snapshots, stats.LastError)
		return false, err
	case "sync":
		if len(fields) != 1 {
			return false, errors.New("usage: sync")
		}
		if err := store.Sync(); err != nil {
			return false, err
		}
		_, err := fmt.Fprintln(output, "OK")
		return false, err
	case "quit", "exit":
		return true, nil
	default:
		return false, fmt.Errorf("unknown command %q; want set, get, delete, stats, sync, or quit", fields[0])
	}
}

type loadFlags struct {
	store       *storeFlags
	readPercent int
	workers     int
	keyspace    uint64
	valueBytes  int
	ttl         time.Duration
}

func runLoad(ctx context.Context, args []string, output, diagnostics io.Writer) error {
	flags := flag.NewFlagSet("load", flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	config := &loadFlags{}
	config.store = addStoreFlags(flags)
	flags.IntVar(&config.readPercent, "read-percent", 0, "percentage of operations that are reads")
	flags.IntVar(&config.workers, "workers", 1, "concurrent workers")
	flags.Uint64Var(&config.keyspace, "keyspace", 10000, "number of keys used by the workload")
	flags.IntVar(&config.valueBytes, "value-bytes", 0, "fixed value size (zero uses the key)")
	flags.DurationVar(&config.ttl, "ttl", 0, "TTL for writes (zero disables TTL)")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if config.readPercent < 0 || config.readPercent > 100 {
		return errors.New("cached load: -read-percent must be between 0 and 100")
	}
	if config.workers <= 0 {
		return errors.New("cached load: -workers must be positive")
	}
	if config.keyspace == 0 {
		return errors.New("cached load: -keyspace must be positive")
	}
	if config.valueBytes < 0 {
		return errors.New("cached load: -value-bytes must not be negative")
	}
	if config.ttl < 0 {
		return errors.New("cached load: -ttl must not be negative")
	}

	store, err := config.store.open(ctx)
	if err != nil {
		return fmt.Errorf("cached load: %w", err)
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var sequence atomic.Uint64
	var outputMu sync.Mutex
	failures := make(chan error, 1)
	var workers sync.WaitGroup
	for worker := range config.workers {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			random := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(worker+1)))
			for workCtx.Err() == nil {
				if random.IntN(100) < config.readPercent {
					key := loadKey(random.Uint64N(config.keyspace) + 1)
					store.Get(key)
					continue
				}
				next := sequence.Add(1)
				key := loadKey((next-1)%config.keyspace + 1)
				value := []byte(key)
				if config.valueBytes != 0 {
					value = make([]byte, config.valueBytes)
					copy(value, key)
				}
				var writeErr error
				if config.ttl == 0 {
					writeErr = store.Set(key, value)
				} else {
					writeErr = store.SetTTL(key, value, config.ttl)
				}
				if writeErr != nil {
					reportLoadFailure(failures, cancel, fmt.Errorf("set %q: %w", key, writeErr))
					return
				}
				outputMu.Lock()
				_, outputErr := fmt.Fprintln(output, key)
				outputMu.Unlock()
				if outputErr != nil {
					reportLoadFailure(failures, cancel, fmt.Errorf("acknowledge %q: %w", key, outputErr))
					return
				}
			}
		}(worker)
	}

	select {
	case <-ctx.Done():
	case err = <-failures:
	}
	cancel()
	workers.Wait()
	stats := store.Stats()
	_, reportErr := fmt.Fprintf(diagnostics, "cached load: reads=%d writes=%d hits=%d misses=%d\n",
		stats.Hits+stats.Misses, stats.RecordsWritten, stats.Hits, stats.Misses)
	return errors.Join(err, reportErr, closeStore(store, diagnostics))
}

func reportLoadFailure(failures chan<- error, cancel context.CancelFunc, err error) {
	select {
	case failures <- err:
	default:
	}
	cancel()
}

func loadKey(sequence uint64) string {
	return fmt.Sprintf("key-%020d", sequence)
}
