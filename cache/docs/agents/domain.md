# Cache

An in-memory key-value store whose contents survive restart. It exists to serve reads at memory speed while keeping a durable account of every change it has accepted.

## Language

### The store

**Store**:
The whole key-value collection together with its durability machinery. The unit that is opened, recovered, and closed.
_Avoid_: Cache (the module), map, database

**Entry**:
A key, its value, and what the store knows about them. Exists from the moment a write is accepted until deletion, expiry, or eviction removes it.
_Avoid_: Item, record (a record is a log term), row

**Key**:
The identifier of at most one entry.

**Value**:
An opaque sequence of bytes belonging to an entry. The store never interprets it.
_Avoid_: Payload, data, blob

**Capacity**:
The upper bound on memory the store is not permitted to exceed.
_Avoid_: Limit, max size, quota

### Lifetime

**TTL**:
The lifetime requested for an entry when it is written.
_Avoid_: Expiry (that is the event), timeout, lifespan

**Deadline**:
The point in time after which an entry is no longer live. An entry written without a TTL has no deadline.
_Avoid_: Expiry time, expires-at

**Live**:
Present in the store with its deadline not yet passed. Only live entries are observable to readers, whether or not the store has reclaimed anything.
_Avoid_: Valid, fresh, active

**Expiry**:
Removal of an entry because its deadline passed. Driven by the entry alone, never by pressure on the store.
_Avoid_: Eviction, timeout, invalidation

**Eviction**:
Removal of a live entry because the store is at capacity and needs room. Driven by pressure alone, never by a deadline.
_Avoid_: Expiry, purge, reaping

### Durability

**Write-ahead log**:
The durable, ordered account of every change the store has accepted, written before that change is acknowledged. It is the authority on the store's contents; memory is a view of it.
_Avoid_: Journal, transaction log, AOF

**Record**:
One accepted change in the write-ahead log.
_Avoid_: Entry (that is an in-memory term), event, message

**Snapshot**:
A complete image of the store as of one point in its history, written so recovery can begin there rather than at the start of the log.
_Avoid_: Checkpoint, dump, backup

**Segment**:
One file of the write-ahead log. The log is a sequence of segments; the newest receives writes and the rest are immutable.
_Avoid_: Chunk, part, log file

**Recovery**:
Rebuilding the store's contents when it is opened, from a snapshot and the records that follow it.
_Avoid_: Replay (that is one step of it), restore, load

**Sweep**:
A pass that looks for entries whose deadlines have passed and reclaims them. A sweep reclaims memory only; it never changes what a reader can observe.
_Avoid_: Cleanup, GC, reaping, scan

**Durability window**:
The span of accepted changes that may be lost to power failure. A change leaves the window once it is on stable storage. The window bounds data loss; it never affects what readers observe.
_Avoid_: Lag, flush window, sync interval

**Torn tail**:
An incomplete record at the end of the log, left by a crash partway through a write. An expected consequence of crashing, not corruption.
_Avoid_: Corruption, partial write, bad record
