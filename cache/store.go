package cache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

// ErrClosed distinguishes writes attempted after a Store has closed.
var ErrClosed = errors.New("cache: store is closed")

type entry struct {
	value []byte
}

type shard struct {
	mu      sync.RWMutex
	entries map[string]entry
}

type errorState struct {
	err error
}

type counters struct {
	hits           atomic.Uint64
	misses         atomic.Uint64
	recordsWritten atomic.Uint64
	bytesWritten   atomic.Uint64
	fsyncs         atomic.Uint64
	lastError      atomic.Pointer[errorState]
}

// Stats is a point-in-time copy of Store activity counters.
type Stats struct {
	Hits           uint64
	Misses         uint64
	Expiries       uint64
	Evictions      uint64
	RecordsWritten uint64
	BytesWritten   uint64
	Fsyncs         uint64
	Snapshots      uint64
	LastError      string
}

// Store is a concurrent in-memory key-value collection backed by a write-ahead log.
type Store struct {
	dir    string
	logger *slog.Logger

	shards    []shard
	shardMask uint64
	entries   atomic.Uint64
	bytes     atomic.Uint64
	stats     counters

	stateMu  sync.RWMutex
	closed   bool
	requests chan *writeRequest
	done     chan struct{}

	lockFile *os.File
}

// Open takes exclusive ownership of dir and recovers its write-ahead log.
func Open(ctx context.Context, dir string, supplied ...Option) (*Store, error) {
	if ctx == nil {
		return nil, errors.New("cache: Open: nil context")
	}
	options := defaultOptions()
	for index, option := range supplied {
		if option == nil {
			return nil, fmt.Errorf("cache: option %d is nil", index)
		}
		if err := option(&options); err != nil {
			return nil, fmt.Errorf("cache: invalid option: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("cache: recover %q: %w", dir, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("cache: create directory %q: %w", dir, err)
	}

	lockFile, err := os.OpenFile(dir+string(os.PathSeparator)+"LOCK", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("cache: open lock for %q: %w", dir, err)
	}
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("cache: store directory %q is already open: %w", dir, err)
	}

	store := &Store{
		dir:       dir,
		logger:    options.logger,
		shards:    make([]shard, options.shards),
		shardMask: uint64(options.shards - 1),
		requests:  make(chan *writeRequest, 1024),
		done:      make(chan struct{}),
		lockFile:  lockFile,
	}
	for index := range store.shards {
		store.shards[index].entries = make(map[string]entry)
	}

	log, err := recoverLog(ctx, store, options.segmentSize)
	if err != nil {
		_ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		_ = lockFile.Close()
		return nil, err
	}
	go store.runWriter(log, options)
	return store, nil
}

// Get returns a copy of the value for key and whether it is present.
func (store *Store) Get(key string) ([]byte, bool) {
	shard := store.shardFor(key)
	shard.mu.RLock()
	item, ok := shard.entries[key]
	if !ok {
		shard.mu.RUnlock()
		store.stats.misses.Add(1)
		return nil, false
	}
	value := append([]byte(nil), item.value...)
	shard.mu.RUnlock()
	store.stats.hits.Add(1)
	return value, true
}

// GetInto appends the value for key to dst[:0] and reports whether it is present.
func (store *Store) GetInto(key string, dst []byte) ([]byte, bool) {
	shard := store.shardFor(key)
	shard.mu.RLock()
	item, ok := shard.entries[key]
	if !ok {
		shard.mu.RUnlock()
		store.stats.misses.Add(1)
		return dst[:0], false
	}
	dst = append(dst[:0], item.value...)
	shard.mu.RUnlock()
	store.stats.hits.Add(1)
	return dst, true
}

// Set durably records value under key before making it visible.
func (store *Store) Set(key string, value []byte) error {
	if err := validateRecordSize(key, len(value)); err != nil {
		return err
	}
	return store.submit(&writeRequest{kind: requestSet, key: key, value: append([]byte(nil), value...), result: make(chan error, 1)})
}

// Delete durably records removal of key. Deleting an absent key succeeds.
func (store *Store) Delete(key string) error {
	if err := validateRecordSize(key, 0); err != nil {
		return err
	}
	return store.submit(&writeRequest{kind: requestDelete, key: key, result: make(chan error, 1)})
}

// Len returns the current number of entries.
func (store *Store) Len() uint64 {
	return store.entries.Load()
}

// Bytes returns the bytes occupied by stored keys and values.
func (store *Store) Bytes() uint64 {
	return store.bytes.Load()
}

// Stats returns a consistent-enough atomic snapshot of activity counters.
func (store *Store) Stats() Stats {
	stats := Stats{
		Hits:           store.stats.hits.Load(),
		Misses:         store.stats.misses.Load(),
		RecordsWritten: store.stats.recordsWritten.Load(),
		BytesWritten:   store.stats.bytesWritten.Load(),
		Fsyncs:         store.stats.fsyncs.Load(),
	}
	if failure := store.stats.lastError.Load(); failure != nil {
		stats.LastError = failure.err.Error()
	}
	return stats
}

// Sync forces every accepted write onto stable storage.
func (store *Store) Sync() error {
	return store.submit(&writeRequest{kind: requestSync, result: make(chan error, 1)})
}

// Close rejects new writes, drains accepted writes, syncs the log, and releases the directory lock.
func (store *Store) Close() error {
	store.stateMu.Lock()
	if store.closed {
		store.stateMu.Unlock()
		return nil
	}
	store.closed = true
	request := &writeRequest{kind: requestClose, result: make(chan error, 1)}
	store.requests <- request
	store.stateMu.Unlock()

	writerErr := <-request.result
	<-store.done
	unlockErr := syscall.Flock(int(store.lockFile.Fd()), syscall.LOCK_UN)
	closeErr := store.lockFile.Close()
	return errors.Join(writerErr, unlockErr, closeErr)
}

func (store *Store) submit(request *writeRequest) error {
	store.stateMu.RLock()
	if store.closed {
		store.stateMu.RUnlock()
		return ErrClosed
	}
	store.requests <- request
	err := <-request.result
	store.stateMu.RUnlock()
	return err
}

func (store *Store) shardFor(key string) *shard {
	return &store.shards[hashKey(key)&store.shardMask]
}

func (store *Store) applySet(key string, value []byte) {
	shard := store.shardFor(key)
	shard.mu.Lock()
	previous, exists := shard.entries[key]
	shard.entries[key] = entry{value: value}
	shard.mu.Unlock()
	if exists {
		if len(value) >= len(previous.value) {
			store.bytes.Add(uint64(len(value) - len(previous.value)))
		} else {
			store.bytes.Add(^uint64(len(previous.value) - len(value) - 1))
		}
		return
	}
	store.entries.Add(1)
	store.bytes.Add(uint64(len(key) + len(value)))
}

func (store *Store) applyDelete(key string) {
	shard := store.shardFor(key)
	shard.mu.Lock()
	previous, exists := shard.entries[key]
	if exists {
		delete(shard.entries, key)
	}
	shard.mu.Unlock()
	if exists {
		store.entries.Add(^uint64(0))
		size := len(key) + len(previous.value)
		if size > 0 {
			store.bytes.Add(^uint64(size - 1))
		}
	}
}

func validateRecordSize(key string, valueLength int) error {
	if len(key) > int(^uint16(0)) {
		return fmt.Errorf("cache: key is too large: %d bytes", len(key))
	}
	if uint64(recordFixedSize)+uint64(len(key))+uint64(valueLength) > uint64(^uint32(0)) {
		return fmt.Errorf("cache: record is too large")
	}
	return nil
}

func (store *Store) latch(err error) error {
	if err == nil {
		return nil
	}
	state := &errorState{err: err}
	if store.stats.lastError.CompareAndSwap(nil, state) {
		return err
	}
	return store.stats.lastError.Load().err
}

func (store *Store) warnTornTail(path string, offset int64) {
	if store.logger != nil {
		store.logger.Warn("trimmed torn tail", "file", path, "offset", offset)
	}
}

func hashKey(key string) uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	hash := offset
	for index := range len(key) {
		hash ^= uint64(key[index])
		hash *= prime
	}
	return hash
}
