# On-disk format

The byte-level contract for a store directory. Decisions behind it live in `docs/adr/`; this file is the specification those decisions imply.

All integers are little-endian.

## Directory

```
<dir>/LOCK               exclusive OS file lock, held for the lifetime of an open store
<dir>/NNNNNNNN.seg       log segment, named for the sequence of its first record
<dir>/NNNNNNNN.snap      snapshot, named for the lowest sequence it covers
```

Segment and snapshot names are zero-padded to 8 digits so lexical order is sequence order. At most one snapshot is retained; segments below its lowest covered sequence are deleted after it is installed.

## Segment

A segment begins with a header and holds records until it exceeds the segment size, at which point the next record starts a new segment.

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

The snapshot is written to a temporary file, fsynced, and installed by rename.

## Recovery

1. Take the lock.
2. Read the newest snapshot. If its shard count differs from the configured one, discard it.
3. Load its entries, then replay segments from the lowest sequence in its header — or from the oldest retained segment if there is no usable snapshot.
4. Skip records already applied; applying a record is idempotent per key.
5. Drop entries whose deadline has passed, then evict down to the configured capacity.

### Failure tiers

| Condition | Response |
| --- | --- |
| Unknown magic, or version above what the binary understands | Refuse to open |
| CRC failure on the last record of the last segment | Truncate to the last good record, warn, continue |
| CRC failure with valid records after it, or a sequence gap | Refuse to open, naming file and offset |
