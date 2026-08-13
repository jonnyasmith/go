# cache

An embeddable key-value store for Go. The durable in-memory store survives process restart, expires stale entries, and stays within a configured memory capacity.

> **Status:** The exported store API, graceful shutdown, terminal REPL, configurable load generator, crash suite, and contention benchmarks are implemented.

```go
c, err := cache.Open(ctx, "/var/lib/myapp/cache",
    cache.WithCapacity(256 << 20),
    cache.WithFlushInterval(time.Second),
    cache.WithSweepInterval(time.Second),
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

Directory ownership is supported on Windows and on the Unix targets named by the Go build constraints. Unix lock and unlock operations retry interrupted system calls while preserving non-blocking contention errors. Unix installations fsync the store directory after renames. Windows uses an explicit fallback because the Go standard library cannot portably flush directory handles: snapshot and segment files are fsynced before rename, but the directory entry itself is not separately flushed.

## Guarantees

**Accepted writes survive a crash.** Every change is appended to a write-ahead log before `Set` returns. Kill the process and reopen: nothing acknowledged is missing.

**Power loss costs at most one flush interval.** An fsync runs on a ticker rather than on every write, so a `Set` does not wait for the disk. The interval is the *durability window* and it is configurable; `Sync` closes it on demand. This is the trade the store makes for write throughput, and it is stated here rather than buried.

**A durability failure is not hidden.** If a log write fails, that write returns the error and the store latches: further writes return the same error and only reopening clears it. Reads keep working so a degraded process can still serve while it is replaced. The store never quietly downgrades to memory-only.

**Values are copied in and out.** `Set` copies what you give it and `Get` returns a copy, so no caller can corrupt the store by holding onto a slice. Use `GetInto` with your own buffer in hot loops. This is a correctness property, not a tuning knob.

**Expiry is exact; reclamation is not.** `SetTTL` requires a positive TTL and converts it to an absolute deadline when the call is accepted. An entry stops being observable the instant its deadline passes, whether or not memory has been reclaimed. A background sweep does the reclaiming in bounded slices so a large store does not stall.

**No context on the data path.** `Get` performs no I/O and a `Set` that has been accepted cannot be un-accepted, so cancellation would be decoration. `Open` takes a context, which bounds recovery.

## Two behaviours that surprise people

**An entry evicted for space comes back after a restart.** Eviction is a decision about *memory*, not about *data*, so nothing durable records it. The log still contains the entry, and recovery restores it. Expired entries never come back, because deadlines are durable and evaluated on every read.

**Eviction is least-recently-used within a shard, not globally.** The store is sharded to avoid a single lock, and each shard evicts its own least-recently-used entries against its share of the ceiling. A badly skewed key distribution can evict from one shard while another has room. The skew is bounded and accepted; exact global LRU would reintroduce the global lock that sharding exists to remove.

## Configuration

Options are passed to `Open` and validated there; an invalid value fails immediately with an error naming the option.

| Option | Controls | Default |
| --- | --- | --- |
| `WithShards` | Shard count, a power of two | `GOMAXPROCS` rounded up |
| `WithCapacity` | Byte ceiling across the store | 256 MiB |
| `WithFlushInterval` | The durability window | 1 second |
| `WithSegmentSize` | When the log rolls to a new file | 64 MiB |
| `WithSnapshotThreshold` | Log bytes written between automatic snapshots | 256 MiB |
| `WithSweepInterval` | How often every shard is considered for reclamation | 1 second |
| `WithLogger` | A `*slog.Logger`; silent by default | no logger |

Capacity is divided evenly across shards and must provide at least the fixed 64-byte overhead per shard. Each entry is charged for its key, value, and that overhead. The sweep visits shards at staggered offsets, samples entries within each shard, repeats samples while at least one quarter are reclaimed, and spends at most one millisecond in a shard per visit.

`Set` and `SetTTL` reject an entry before submission when its charged key, value, and overhead cannot fit in the target shard. Rejection does not append to the WAL, evict another entry, or change activity counters.

Capacity and shard count may be changed between runs. A snapshot written with another shard count is loaded as the durable base image; recovery disables unsafe per-shard replay skipping and retains enough WAL history to replay every record after the snapshot's lowest sequence. The snapshot is not discarded, and retained history remains required to fill the skew between its shard sequences.

Eviction never becomes a durable deletion. After any live-entry eviction, each automatic compaction rolls the writer to a new segment, reconstructs the durable image from the installed snapshot and immutable WAL prefix, and snapshots that image before pruning only the covered prefix. The configured snapshot threshold remains the compaction trigger under sustained pressure: retained WAL is bounded by that threshold plus records accepted while one compaction runs. Clean shutdown performs the same durable-image reconstruction, so reopening with greater capacity restores live entries that had been evicted from memory.

## Observability

`Stats` returns a plain struct read from atomic counters — hits, misses, expiries, evictions, records and bytes written, fsyncs, snapshots, and the last error. Export it however your program already exports things. The module deliberately offers no metrics interface and imports no metrics library: an indirect call on every cache hit would cost more than the counter feeding it.

## On disk

A store owns a directory containing a lock file, the log as a sequence of segments, and at most one snapshot. Snapshots are assembled one shard at a time and record the log sequence reached by each shard. Recovery loads the snapshot as a base image, replays from its lowest recorded sequence, and tolerates records already represented in later shards. Snapshots are written automatically after the configured log-byte threshold and on clean shutdown, using a temporary file, fsync, and rename; superseded segments are deleted only after installation.

Damaged files are graded rather than treated alike. An incomplete record at the end of the log is what a crash mid-write looks like: it is trimmed and the store opens. Corruption with valid records after it, or a gap in the sequence, is real data loss: the store refuses to open and names the file and offset. There is no repair mode — a store that silently discards part of its history is worse than one that will not start.

The byte-level layout is specified in [docs/on-disk-format.md](docs/on-disk-format.md).

## Command

`cmd/cached repl -dir DIRECTORY` opens a terminal REPL. Commands are `set KEY VALUE [TTL]`, `get KEY`, `delete KEY`, `stats`, `sync`, and `quit`; TTLs use Go duration syntax such as `30s` or `5m`.

`cmd/cached load -dir DIRECTORY -read-percent 90 -workers 8` drives a configurable concurrent read/write mix until interrupted. `-keyspace`, `-capacity`, `-shards`, `-snapshot-threshold`, `-value-bytes`, and `-ttl` make expiry, eviction, snapshot installation, and shutdown observable under load. Interrupt and termination signals stop the workload, run the store's full close sequence, and report whether closure succeeded. The command opens no listener and implements no wire protocol.

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

The durable store, lifetime and capacity management, segmented log, snapshots, graceful shutdown, terminal REPL, load generator, crash suite, and contention benchmarks are implemented:

- [`docs/agents/domain.md`](docs/agents/domain.md) — vocabulary
- [`docs/adr/`](docs/adr/) — decisions and the reasoning behind them
- [`docs/on-disk-format.md`](docs/on-disk-format.md) — the byte-level contract

Where this README and those documents disagree, they are right and this is stale.
