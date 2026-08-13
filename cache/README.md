# cache

An embeddable key-value store for Go. The durable map core survives process restart; expiry and memory-capacity work remain planned.

> **Status:** `Open`, the map operations, write-ahead log recovery, synchronization, statistics, and load-mode command are implemented. TTL, capacity eviction, sweeping, and snapshots remain specified but are not yet implemented.

```go
c, err := cache.Open(ctx, "/var/lib/myapp/cache",
    cache.WithFlushInterval(time.Second),
)
if err != nil {
    return err
}
defer c.Close()

if err := c.Set("user:42", payload); err != nil {
    return err
}

if v, ok := c.Get("user:42"); ok {
    use(v)
}
```

## Who this is for

A single Go service that reads from something slower or costlier than itself — a database, a metered third-party API, object storage, an inference endpoint — and wants that layer hit less often.

Against a `map` behind a `sync.RWMutex`, which is what most programs start with, this adds expiry, a memory bound, and survival across restart. The last one is the point. A service deployed ten times a day with an in-memory map has ten cold starts a day, each one a stampede onto whatever the cache was protecting. This one comes back warm.

Against Redis or Memcached, this removes the network hop, the serialisation, and the extra process to deploy and monitor. A local lookup is tens of nanoseconds against a few hundred microseconds over a loopback socket.

Against the well-tuned in-memory caches in the Go ecosystem, the difference is durability: they are faster and better-shaped for pure in-memory work, and none of them survive a restart. If restart warmth is not worth anything to you, use one of those instead.

### When to use something else

| Situation | Use instead |
| --- | --- |
| More than one process or machine needs the same cache | Redis, Memcached |
| Losing recent writes is unacceptable | A database |
| The data is cheap to recompute and restarts are rare | A `map` and a mutex |
| You need transactions, queries, or secondary indexes | A database |

A store directory is owned by exactly one process, enforced by a lock file. Two processes cannot share one.

## Guarantees

**Accepted writes survive a crash.** Every change is appended to a write-ahead log before `Set` returns. Kill the process and reopen: nothing acknowledged is missing.

**Power loss costs at most one flush interval.** An fsync runs on a ticker rather than on every write, so a `Set` does not wait for the disk. The interval is the *durability window* and it is configurable; `Sync` closes it on demand. This is the trade the store makes for write throughput, and it is stated here rather than buried.

**A durability failure is not hidden.** If a log write fails, that write returns the error and the store latches: further writes return the same error and only reopening clears it. Reads keep working so a degraded process can still serve while it is replaced. The store never quietly downgrades to memory-only.

**Values are copied in and out.** `Set` copies what you give it and `Get` returns a copy, so no caller can corrupt the store by holding onto a slice. Use `GetInto` with your own buffer in hot loops. This is a correctness property, not a tuning knob.

**Expiry is exact; reclamation is not.** An entry stops being observable the instant its deadline passes, whether or not memory has been reclaimed. A background sweep does the reclaiming in bounded slices so a large store does not stall.

**No context on the data path.** `Get` performs no I/O and a `Set` that has been accepted cannot be un-accepted, so cancellation would be decoration. `Open` takes a context, which bounds recovery.

## Two behaviours that surprise people

**An entry evicted for space comes back after a restart.** Eviction is a decision about *memory*, not about *data*, so nothing durable records it. The log still contains the entry, and recovery restores it. Expired entries never come back, because deadlines are durable and evaluated on every read.

**Eviction is least-recently-used within a shard, not globally.** The store is sharded to avoid a single lock, and each shard evicts its own least-recently-used entries against its share of the ceiling. A badly skewed key distribution can evict from one shard while another has room. The skew is bounded and accepted; exact global LRU would reintroduce the global lock that sharding exists to remove.

## Configuration

Options are passed to `Open` and validated there; an invalid value fails immediately with an error naming the option.

| Option | Controls | Status |
| --- | --- | --- |
| `WithShards` | Shard count, a power of two; defaults to `GOMAXPROCS` rounded up | Implemented |
| `WithFlushInterval` | The durability window | Implemented |
| `WithSegmentSize` | When the log rolls to a new file | Implemented |
| `WithCapacity` | Byte ceiling across the store — key, value, and per-entry overhead | Planned |
| `WithSweepInterval` | How often expired entries are reclaimed | Planned |
| `WithLogger` | A `*slog.Logger`; silent by default | Implemented |

When capacity and snapshots are implemented, capacity and shard count may both be changed between runs without losing data. A changed shard count will discard the snapshot and replay the log instead, costing a slower start and nothing else.

## Observability

`Stats` returns a plain struct read from atomic counters — hits, misses, expiries, evictions, records and bytes written, fsyncs, snapshots, and the last error. Export it however your program already exports things. The module deliberately offers no metrics interface and imports no metrics library: an indirect call on every cache hit would cost more than the counter feeding it.

## On disk

A store owns a directory containing a lock file, the log as a sequence of segments, and at most one snapshot. Recovery loads the snapshot and replays the segments that follow it, so start-up time tracks recent write volume rather than the store's whole history. Superseded segments are deleted once a snapshot is installed.

Damaged files are graded rather than treated alike. An incomplete record at the end of the log is what a crash mid-write looks like: it is trimmed and the store opens. Corruption with valid records after it, or a gap in the sequence, is real data loss: the store refuses to open and names the file and offset. There is no repair mode — a store that silently discards part of its history is worse than one that will not start.

The byte-level layout is specified in [docs/on-disk-format.md](docs/on-disk-format.md).

## Command

`cmd/cached load -dir DIRECTORY` drives concurrent-safe durable writes until it is interrupted or killed. It is used by the crash-injection test. The terminal REPL and configurable read/write mix are planned.

## Dependencies

The standard library, and nothing else — for the runtime and for the tests.

## Requirements

Go 1.26 or later.

## Build and test

```bash
go build ./...
go test -race ./...
```

## Status

The durable-store scope is implemented. TTL expiry, capacity and LRU eviction, background sweeping, and snapshots remain planned:

- [`docs/agents/domain.md`](docs/agents/domain.md) — vocabulary
- [`docs/adr/`](docs/adr/) — decisions and the reasoning behind them
- [`docs/on-disk-format.md`](docs/on-disk-format.md) — the byte-level contract

Where this README and those documents disagree, they are right and this is stale.
