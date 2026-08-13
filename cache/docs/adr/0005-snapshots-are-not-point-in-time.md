# Snapshots are assembled shard by shard, not frozen

Go cannot fork, so there is no cheap copy-on-write image of the store, and taking the global write lock long enough to serialise a large store would stall every caller for hundreds of milliseconds. Instead each shard is serialised under its own read lock, and the snapshot header records the log sequence number reached by each shard.

## Consequences

- The snapshot is not a single point in time; shards in it are internally consistent but mutually skewed.
- Recovery corrects for this by replaying the log from the **lowest** sequence in the header. Records for shards already past that point are re-applied harmlessly, because applying a record is idempotent per key.
- A snapshot attempt records the eviction generation before serialization and validates it under the eviction-path interlock before installation. If a live-entry eviction overlaps serialization, the attempt is discarded before rename and log pruning; serialization itself still takes only per-shard read locks.
- Because the header holds one sequence per shard, it also records the original shard count. A store reopened with a different shard count keeps the snapshot as its base image but cannot map the old per-shard replay positions onto the new shards. It safely skips only through the lowest header sequence, then replays every retained record after that point.
