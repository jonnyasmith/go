# Snapshots are assembled shard by shard, not frozen

Go cannot fork, so there is no cheap copy-on-write image of the store, and taking the global write lock long enough to serialise a large store would stall every caller for hundreds of milliseconds. Instead each shard is serialised under its own read lock, and the snapshot header records the log sequence number reached by each shard.

## Consequences

- The snapshot is not a single point in time; shards in it are internally consistent but mutually skewed.
- Recovery corrects for this by replaying the log from the **lowest** sequence in the header. Records for shards already past that point are re-applied harmlessly, because applying a record is idempotent per key.
- A snapshot is written to a temporary file, fsynced, and installed by rename, so a partial snapshot is never visible to recovery.
- Because the header holds one sequence per shard, it also fixes the shard count. A store reopened with a different shard count discards the snapshot and replays from the oldest retained segment rather than failing: retuning sharding is a legitimate operational change, and the only cost is a slower recovery.
