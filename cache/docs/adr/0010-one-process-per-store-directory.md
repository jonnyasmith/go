# A store directory is held by exactly one process

`Open` takes an exclusive, non-blocking operating-system lock on a `LOCK` file in the store directory and fails immediately if it is already held. Unix systems use `flock`; Windows uses `LockFileEx`. Two processes appending to the same log interleave records and destroy it, and the failure is silent until recovery, so the store refuses the situation outright rather than detecting it later.

## Consequences

- There is no timeout, no retry, and no stale-lock detection. The operating system releases the lock when the process dies or its handle closes, so a crash never leaves a lock to clean up and any heuristic for "stale" would only exist to break a lock that is genuinely held.
- The error names the directory, because the usual cause is a second copy of the same program.
