package cache

import (
	"fmt"
	"log/slog"
	"runtime"
	"time"
)

const (
	defaultCapacity          = uint64(256 << 20)
	defaultFlushInterval     = time.Second
	defaultSegmentSize       = int64(64 << 20)
	defaultSnapshotThreshold = int64(256 << 20)
	defaultSweepInterval     = time.Second
)

type options struct {
	shards            int
	capacity          uint64
	flushInterval     time.Duration
	segmentSize       int64
	snapshotThreshold int64
	sweepInterval     time.Duration
	logger            *slog.Logger
}

// Option configures a Store when it is opened.
type Option func(*options) error

// WithShards sets the number of independently locked key shards.
func WithShards(count int) Option {
	return func(options *options) error {
		if count <= 0 || count&(count-1) != 0 {
			return fmt.Errorf("WithShards: count must be a positive power of two")
		}
		options.shards = count
		return nil
	}
}

// WithCapacity sets the byte ceiling across keys, values, and fixed entry overhead.
func WithCapacity(bytes uint64) Option {
	return func(options *options) error {
		if bytes == 0 {
			return fmt.Errorf("WithCapacity: capacity must be positive")
		}
		options.capacity = bytes
		return nil
	}
}

// WithFlushInterval sets the maximum durability window between fsyncs.
func WithFlushInterval(interval time.Duration) Option {
	return func(options *options) error {
		if interval <= 0 {
			return fmt.Errorf("WithFlushInterval: interval must be positive")
		}
		options.flushInterval = interval
		return nil
	}
}

// WithSegmentSize sets the write-ahead log size at which the next record starts a new segment.
func WithSegmentSize(size int64) Option {
	return func(options *options) error {
		if size <= segmentHeaderSize {
			return fmt.Errorf("WithSegmentSize: size must exceed %d bytes", segmentHeaderSize)
		}
		options.segmentSize = size
		return nil
	}
}

// WithSnapshotThreshold sets the log bytes written between automatic snapshots.
func WithSnapshotThreshold(bytes int64) Option {
	return func(options *options) error {
		if bytes <= 0 {
			return fmt.Errorf("WithSnapshotThreshold: bytes must be positive")
		}
		options.snapshotThreshold = bytes
		return nil
	}
}

// WithSweepInterval sets how often every shard is considered for expired-entry reclamation.
func WithSweepInterval(interval time.Duration) Option {
	return func(options *options) error {
		if interval <= 0 {
			return fmt.Errorf("WithSweepInterval: interval must be positive")
		}
		options.sweepInterval = interval
		return nil
	}
}

// WithLogger sets the logger used for recovery warnings. The Store is silent by default.
func WithLogger(logger *slog.Logger) Option {
	return func(options *options) error {
		if logger == nil {
			return fmt.Errorf("WithLogger: logger must not be nil")
		}
		options.logger = logger
		return nil
	}
}

func defaultOptions() options {
	shards := 1
	for shards < runtime.GOMAXPROCS(0) {
		shards <<= 1
	}
	return options{
		shards:            shards,
		capacity:          defaultCapacity,
		flushInterval:     defaultFlushInterval,
		segmentSize:       defaultSegmentSize,
		snapshotThreshold: defaultSnapshotThreshold,
		sweepInterval:     defaultSweepInterval,
	}
}
