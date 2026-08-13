# Records carry absolute deadlines, not remaining TTLs

A record stores the wall-clock deadline computed when the write was accepted, never the TTL the caller passed. A relative TTL would be re-based against the clock at recovery time, so every restart would resurrect entries that should have died and a crash loop would keep them alive indefinitely.

## Consequences

- Replay is deterministic: the same log always recovers the same live set for a given recovery time.
- The store inherits the wall clock's behaviour. A backwards clock step extends deadlines and a forwards step retires entries early; neither corrupts the store, and neither is compensated for.
- A record is otherwise a length prefix, a CRC32C (Castagnoli) over everything the prefix covers, a closed operation enum, a key length, and a monotonic sequence number, behind a versioned file header. The value length is derived from the record length rather than stored, so a record cannot disagree with itself about its own size.
- Absolute deadlines leave a delete record nothing to carry, and recovery enforces that rather than tolerating it: a delete record bearing a deadline or a value is a malformed log and refuses the open.
