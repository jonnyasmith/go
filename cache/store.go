package cache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrClosed distinguishes writes attempted after a Store has closed.
	ErrClosed = errors.New("cache: store is closed")

	errDirectoryLockHeld = errors.New("directory lock is held")
)

type entry struct {
	key      string
	value    []byte
	deadline int64
	sequence uint64
	previous *entry
	next     *entry
}

type shard struct {
	mu       sync.RWMutex
	entries  map[string]*entry
	recent   *entry
	oldest   *entry
	bytes    uint64
	capacity uint64
}

type errorState struct {
	err error
}

type counters struct {
	hits           atomic.Uint64
	misses         atomic.Uint64
	expiries       atomic.Uint64
	evictions      atomic.Uint64
	recordsWritten atomic.Uint64
	bytesWritten   atomic.Uint64
	fsyncs         atomic.Uint64
	snapshots      atomic.Uint64
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
	bytes     atomic.Int64
	stats     counters

	stateMu   sync.RWMutex
	closed    bool
	requests  chan *writeRequest
	done      chan struct{}
	sweepStop chan struct{}
	sweepDone chan struct{}

	logSequence     atomic.Uint64
	snapshotRunning atomic.Bool
	snapshotMu      sync.Mutex
	snapshotWG      sync.WaitGroup

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
	if options.capacity/uint64(options.shards) < entryOverhead {
		return nil, fmt.Errorf("cache: invalid option: WithCapacity: capacity per shard must be at least %d bytes", entryOverhead)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("cache: recover %q: %w", dir, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("cache: create directory %q: %w", dir, err)
	}

	lockFile, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("cache: open lock for %q: %w", dir, err)
	}
	if err := acquireDirectoryLock(lockFile); err != nil {
		_ = lockFile.Close()
		if errors.Is(err, errDirectoryLockHeld) {
			return nil, fmt.Errorf("cache: store directory %q is already open: %w", dir, err)
		}
		return nil, fmt.Errorf("cache: lock store directory %q: %w", dir, err)
	}

	shardCapacity := options.capacity / uint64(options.shards)
	store := &Store{
		dir:       dir,
		logger:    options.logger,
		shards:    make([]shard, options.shards),
		shardMask: uint64(options.shards - 1),
		requests:  make(chan *writeRequest, 1024),
		done:      make(chan struct{}),
		sweepStop: make(chan struct{}),
		sweepDone: make(chan struct{}),
		lockFile:  lockFile,
	}
	for index := range store.shards {
		store.shards[index].entries = make(map[string]*entry)
		store.shards[index].capacity = shardCapacity
	}

	log, err := recoverLog(ctx, store)
	if err != nil {
		_ = releaseDirectoryLock(lockFile)
		_ = lockFile.Close()
		return nil, err
	}
	store.enforceRecoveredState(time.Now().UnixNano())
	go store.runWriter(log, options)
	go store.runSweep(options.sweepInterval)
	return store, nil
}

// Get returns a copy of the value for key and whether it is present.
func (store *Store) Get(key string) ([]byte, bool) {
	return store.getInto(key, nil)
}

// GetInto appends the value for key to dst[:0] and reports whether it is present.
func (store *Store) GetInto(key string, dst []byte) ([]byte, bool) {
	return store.getInto(key, dst)
}

// Set durably records value under key before making it visible.
func (store *Store) Set(key string, value []byte) error {
	if err := validateRecordSize(key, len(value)); err != nil {
		return err
	}
	return store.submit(&writeRequest{kind: requestSet, key: key, value: append([]byte(nil), value...), result: make(chan error, 1)})
}

// SetTTL durably records value under key with a positive TTL converted to an accept-time deadline.
func (store *Store) SetTTL(key string, value []byte, ttl time.Duration) error {
	if ttl <= 0 {
		return errors.New("cache: SetTTL: ttl must be positive")
	}
	if err := validateRecordSize(key, len(value)); err != nil {
		return err
	}
	deadline := time.Now().Add(ttl).UnixNano()
	return store.submit(&writeRequest{
		kind:     requestSet,
		key:      key,
		value:    append([]byte(nil), value...),
		deadline: deadline,
		result:   make(chan error, 1),
	})
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

// Bytes returns the bytes charged for keys, values, and fixed per-entry overhead.
func (store *Store) Bytes() uint64 {
	return uint64(store.bytes.Load())
}

// Stats returns a consistent-enough atomic snapshot of activity counters.
func (store *Store) Stats() Stats {
	stats := Stats{
		Hits:           store.stats.hits.Load(),
		Misses:         store.stats.misses.Load(),
		Expiries:       store.stats.expiries.Load(),
		Evictions:      store.stats.evictions.Load(),
		RecordsWritten: store.stats.recordsWritten.Load(),
		BytesWritten:   store.stats.bytesWritten.Load(),
		Fsyncs:         store.stats.fsyncs.Load(),
		Snapshots:      store.stats.snapshots.Load(),
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
	close(store.sweepStop)
	store.stateMu.Unlock()
	<-store.sweepDone

	request := &writeRequest{kind: requestClose, result: make(chan error, 1)}
	store.requests <- request
	writerErr := <-request.result
	<-store.done
	unlockErr := releaseDirectoryLock(store.lockFile)
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
	store.stateMu.RUnlock()
	return <-request.result
}

func (store *Store) shardFor(key string) *shard {
	return &store.shards[hashKey(key)&store.shardMask]
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
