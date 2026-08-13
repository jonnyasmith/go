# Records carry absolute deadlines, not remaining TTLs

A record stores the wall-clock deadline computed when the write was accepted, never the TTL the caller passed. A relative TTL would be re-based against the clock at recovery time, so every restart would resurrect entries that should have died and a crash loop would keep them alive indefinitely.

## Consequences

- Replay is deterministic: the same log always recovers the same live set for a given recovery time.
- The store inherits the wall clock's behaviour. A backwards clock step extends deadlines and a forwards step retires entries early; neither corrupts the store, and neither is compensated for.
- Records are otherwise a length prefix, a CRC32C (Castagnoli) over the payload, a closed operation enum, and a monotonic sequence number, behind a versioned file header.
