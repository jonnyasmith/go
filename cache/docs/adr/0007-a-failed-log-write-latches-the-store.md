# A failed log write puts the store into a terminal error state

When a write to the log fails — disk full, I/O error, the file removed underneath the process — the write is rejected and the store latches: every subsequent write returns the same error, reads continue to be served, and only reopening clears it. Degrading to memory-only would leave a store that answers reads and accepts writes while its durability guarantee has quietly stopped holding, which is a worse failure than stopping.

## Consequences

- Callers must treat a write error as fatal to the store, not as retryable.
- Reads remain available so a process in this state can still serve traffic while it is being replaced.
