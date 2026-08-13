# On-disk format

The byte-level contract for a store directory. Decisions behind it live in `docs/adr/`; this file is the specification those decisions imply.

All integers are little-endian.

## Directory

```
<dir>/LOCK                           exclusive OS file lock, held for the lifetime of an open store
<dir>/NNNNNNNNNNNNNNNNNNNN.seg       log segment, named for the sequence of its first record
<dir>/NNNNNNNNNNNNNNNNNNNN.snap      snapshot, named for the lowest sequence it covers
```

New segment and snapshot names encode the sequence as zero-padded, fixed-width 20-digit decimal. This represents the complete `uint64` range and makes lexical order equal sequence order.

Eight-digit names written by earlier binaries remain valid when their stem represents the same decimal sequence. Recovery may contain both legacy eight-digit and current 20-digit names and orders them by the parsed numeric sequence; two names with different widths that identify the same sequence are rejected as ambiguous. Existing files are not renamed in place. Every newly installed segment or snapshot uses the 20-digit form, so stores migrate naturally as files are rolled and superseded.

At most one snapshot is retained; segments below its lowest covered sequence are deleted after it is installed.

On the supported Unix targets, every segment or snapshot rename is followed by an fsync of the store directory. On Windows, the file is fsynced before rename, but directory sync is an explicit fallback no-op because Go's standard library does not portably expose flushable directory handles. Unsupported locking targets reject `Open` before installation can begin.

## Segment

A segment begins with a header and holds records until it exceeds the segment size, at which point the next record starts a new segment.

A new segment is created under a temporary name. Its header is written and fsynced, the file is renamed to its sequence name, and the store directory is fsynced before any record is written to the segment. A failure in any installation step is a fatal write-ahead-log failure.

| Field | Type | Notes |
| --- | --- | --- |
| magic | `[4]byte` | `CWAL`, identifying a log segment |
| version | `uint16` | `1`; a version above what the binary knows is fatal |
| flags | `uint16` | reserved, zero |

### Record

| Field | Type | Notes |
| --- | --- | --- |
| length | `uint32` | byte count of everything after this field |
| crc | `uint32` | CRC32C (Castagnoli) over everything after this field |
| op | `uint8` | closed enum: `1` set, `2` delete |
| keyLen | `uint16` | |
| deadline | `int64` | absolute Unix nanoseconds; `0` means no deadline |
| seq | `uint64` | monotonic across the whole log |
| key | `keyLen` bytes | |
| value | remainder | length derived from `length`; empty for delete |

The value length is derived, never stored. Deadlines are absolute and computed when the write is accepted.

## Snapshot

| Field | Type | Notes |
| --- | --- | --- |
| magic | `[4]byte` | `CSNP`, identifying a snapshot |
| version | `uint16` | `1`; a version above what the binary knows is fatal |
| flags | `uint16` | reserved, zero |
| shards | `uint32` | count the snapshot was written with |
| seq | `uint64 × shards` | sequence reached per shard |

Entries follow, grouped by shard:

| Field | Type | Notes |
| --- | --- | --- |
| length | `uint32` | byte count of everything after this field |
| crc | `uint32` | CRC32C (Castagnoli) over everything after this field |
| keyLen | `uint16` | |
| deadline | `int64` | absolute Unix nanoseconds; `0` means no deadline |
| seq | `uint64` | sequence of the change represented by the entry |
| key | `keyLen` bytes | |
| value | remainder | length derived from `length` |

The snapshot is written to a temporary file and fsynced. Before any eviction, serialization records the eviction generation before reading shards; installation takes the eviction-path interlock and validates that generation before rename, and an attempt overlapping a live-entry eviction is discarded without installing the snapshot or pruning segments. After an eviction, automatic compaction rolls the active WAL, reconstructs the durable image through that immutable prefix, and installs a snapshot whose equal shard sequences identify the exact covered boundary. Concurrent writes continue in the new segment, and pruning cannot remove that segment or any newer record. A successful rename is followed by the platform-specific directory installation behavior above.

## Recovery

1. Take the lock.
2. Read the newest snapshot as a base image. If its shard count matches the configured count, retain its per-shard sequences for replay skipping. If the count differs, keep the loaded entries as the base image but disable per-shard skipping.
3. Validate retained history. Without a snapshot, the oldest retained record must be sequence 1. With a snapshot, retained history must begin no later than one past the lowest per-shard sequence and continue through the highest per-shard sequence. No WAL is required only when every per-shard sequence is equal.
4. Replay segments. Matching shard counts skip records represented by each shard. Changed shard counts skip only the prefix through the lowest sequence, which every snapshot shard safely represents, then replay all retained records.
5. Drop entries whose deadline has passed, then evict down to the configured capacity.

### Failure tiers

| Condition | Response |
| --- | --- |
| Unknown magic, or version above what the binary understands | Refuse to open |
| CRC failure on the last record of the last segment | Truncate to the last good record, warn, continue |
| CRC failure with valid records after it, or a sequence gap | Refuse to open, naming file and offset |
