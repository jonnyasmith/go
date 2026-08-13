# Acknowledge writes before they are on stable storage

The store must be fast under concurrent writes and must not lose data on process crash, but an fsync per write costs milliseconds and puts a disk round trip on every caller. A single writer goroutine owns the log file: it drains everything queued into one batched write, and `Set` returns once its record is written to the operating system rather than once it is fsynced. A ticker fsyncs on a configurable interval, and `Sync` forces one on demand.

## Consequences

- A process crash loses nothing; a power failure loses at most one flush interval. That span is the durability window and the README must state it rather than claim "persistent".
- Because the writer goroutine reports the result of the write back to the caller, a disk error surfaces to the caller whose write hit it, not to a background log line.
- The writer is the only owner of the file offset and the sequence counter, so ordering needs no lock.
