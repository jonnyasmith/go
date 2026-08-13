# Expiry and eviction write nothing to the log

Both remove an entry, and neither writes a record. Deadlines are already durable in the record that created the entry, so recovery can re-evaluate expiry itself and remains deterministic without help; logging expiries would double the log for a store of short-lived keys that no caller is writing to. Eviction removes an entry the log still describes, so it changes what this process holds in memory rather than what the store durably contains, and the log describes the latter.

## Consequences

- An entry evicted while still live **reappears on recovery**, because nothing durable ever said it left. This is intended: eviction reflects the memory pressure of one process lifetime, not a change to the store's contents.
- Recovery therefore restores a store that may immediately exceed capacity, and evicts down to the bound as part of opening.
- A reader can never observe a resurrected entry that should have expired, since expiry is evaluated against the deadline on every read.
- Expiry is reclaimed proactively as well as on read. A background sweep visits shards at staggered offsets and samples entries within each, repeating while the sample keeps paying and stopping on a per-shard time budget. It writes nothing and changes nothing a reader can observe; it exists so a store of expired keys releases memory without waiting to be read.
- Writing no record is not the same as eviction being free. Because the log outlives the evicted entry, an image serialized from memory after an eviction no longer describes the log, so the store counts evictions and switches its snapshot path once the count moves. ADR 0005 owns that mechanism; the decision here is only that no record is written.
