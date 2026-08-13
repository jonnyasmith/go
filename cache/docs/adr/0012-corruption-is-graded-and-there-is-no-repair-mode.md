# Damaged files are graded, and there is no repair mode

A crash during an append leaves a partial record at the end of the log. That is the expected shape of a crash, not corruption, and refusing to open because of it would make every hard kill a manual recovery. Everything else is treated as data loss and refuses the open.

## Consequences

- A torn tail is trimmed and the store opens. This covers the whole family of ways the last write can be cut short — a length prefix too short to read, a length that runs past the end of the file, a record whose CRC fails — and only in the final segment, at its very end.
- Anything else refuses `Open` and names the file and the offset: a CRC failure with valid records after it, a gap in the sequence, a segment whose first record disagrees with its name, a record too short or too long to be one, an unknown operation. Each of these means the log is not what the store wrote, and continuing means serving a truncated history as if it were complete.
- Snapshots get no tail tolerance at all. A snapshot is written to a temporary file and fsynced before it is renamed into place, so a short or failing one was never a valid installation and there is nothing to salvage — the log behind it still is.
- There is no repair mode and no flag to force an open. A store that silently discards part of its history is worse than one that will not start: the operator who can decide whether that history matters is not the process.
- The torn-tail trim is reported through the configured logger, and a store with no logger is silent by default. Nothing about the recovery decision depends on that; it is the difference between the event being observable and it being announced.
