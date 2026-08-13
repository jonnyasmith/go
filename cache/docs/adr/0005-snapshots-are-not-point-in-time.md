# Snapshots are assembled shard by shard, not frozen

Go cannot fork, so there is no cheap copy-on-write image of the store, and taking the global write lock long enough to serialise a large store would stall every caller for hundreds of milliseconds. Instead each shard is serialised under its own read lock, and the snapshot header records the log sequence number reached by each shard.

## Consequences

- The snapshot is not a single point in time; shards in it are internally consistent but mutually skewed.
- Recovery corrects for this by replaying the log from the **lowest** sequence in the header. Records for shards already past that point are re-applied harmlessly, because applying a record is idempotent per key.
- A snapshot attempt records the eviction generation before serialization and validates it under the eviction-path interlock before installation. If a live-entry eviction overlaps serialization, the attempt is discarded before rename and log pruning; serialization itself still takes only per-shard read locks.
- Once a live entry has been evicted, automatic compaction no longer snapshots the capacity-constrained in-memory view. The writer rolls to a new segment, a background pass reconstructs the durable image through the closed WAL prefix, and only that prefix can be pruned after installation. Writes continue in the new segment while reconstruction runs.
- After eviction, final shutdown reconstruction processes one configured shard at a time. This bounds additional live-entry memory by the largest durable shard rather than a second full store, at the cost of reading the snapshot and retained WAL once per configured shard before writing the final image.
- The successful-snapshot counter marks completed installation and cleanup: rename, directory synchronization, removal of superseded snapshots, and removal of covered segments. Post-rename failures identify the snapshot as installed but do not increment the counter.
- Because the header holds one sequence per shard, it also records the original shard count. A store reopened with a different shard count keeps the snapshot as its base image but cannot map the old per-shard replay positions onto the new shards. It safely skips only through the lowest header sequence, then replays every retained record after that point.
