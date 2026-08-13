# A failed log write puts the store into a terminal error state

When a write to the log fails — disk full, I/O error, the file removed underneath the process — the write is rejected and the store latches: every subsequent write returns the same error, reads continue to be served, and only reopening clears it. Degrading to memory-only would leave a store that answers reads and accepts writes while its durability guarantee has quietly stopped holding, which is a worse failure than stopping.

## Consequences

- Callers must treat a write error as fatal to the store, not as retryable.
- Reads remain available so a process in this state can still serve traffic while it is being replaced.
- Only log writes latch. A failed snapshot is recorded separately and is not terminal, because the log still holds everything the snapshot would have summarised: the next attempt clears the recorded error and the cost of failing is a longer recovery, not lost data. `Stats` reports the log error when there is one and falls back to the snapshot error otherwise, so the terminal condition is never hidden behind the recoverable one.
- Once latched, the flush ticker stops fsyncing. There is nothing left to make durable that the caller has not already been told about, and retrying a failed device on a timer would turn one reported error into a stream of unreported ones.
