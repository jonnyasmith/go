# Shard the store, and evict per shard

One `RWMutex` over the whole store is a contention wall under mixed read/write load, so the store is split into a power-of-two number of shards keyed by key hash, defaulting to `GOMAXPROCS` rounded up. Capacity is measured in bytes — key, value, and a fixed per-entry overhead — because callers care about memory rather than cardinality, and the global bound is divided evenly across shards.

## Considered Options

Exact global LRU was rejected: a single recency list shared by all shards reinstates the global lock that sharding exists to remove.

## Consequences

- Eviction order is LRU within a shard and only approximately LRU globally.
- A read takes its shard's exclusive lock long enough to update exact recency. Reads in one shard therefore serialize; sharding contains that contention without weakening the LRU guarantee.
- A pathological key distribution can push one shard to its slice of the bound while others are near empty, evicting earlier than a global bound would. This skew is bounded and documented, not corrected.
- The entry count and byte total are maintained atomically so a global size can be read without touching every shard lock.
- Dividing the bound makes entry size an admission decision. `Open` rejects a capacity that cannot cover one entry's fixed overhead per shard, and a write whose charged bytes exceed a single shard's slice is rejected before it reaches the log rather than evicting a shard empty trying to fit.
- The shard hash is frozen. Snapshot loading re-derives each entry's shard from its key, so changing the hash function would silently misplace every entry in an existing snapshot. It is written inline rather than taken from `hash/maphash`, whose per-process seed would break exactly this.
