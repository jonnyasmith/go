package cache

import (
	"fmt"
	"log/slog"
	"runtime"
	"time"
)

const (
	defaultFlushInterval = time.Second
	defaultSegmentSize   = int64(64 << 20)
)

type options struct {
	shards        int
	flushInterval time.Duration
	segmentSize   int64
	logger        *slog.Logger
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
		shards:        shards,
		flushInterval: defaultFlushInterval,
		segmentSize:   defaultSegmentSize,
	}
}
