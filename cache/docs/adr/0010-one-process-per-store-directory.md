# A store directory is held by exactly one process

`Open` takes an exclusive, non-blocking operating-system lock on a `LOCK` file in the store directory and fails immediately if it is already held. Supported Unix systems use `flock`; Windows uses `LockFileEx`. Targets without either standard-library mechanism fail `Open` with an unsupported-locking error rather than running without ownership protection. Two processes appending to the same log interleave records and destroy it, and the failure is silent until recovery, so the store refuses the situation outright rather than detecting it later.

## Consequences

- There is no timeout, no retry, and no stale-lock detection. Unix kernels release `flock` when the process dies or its descriptor closes. Windows also releases byte-range locks after process death or handle close, but Microsoft documents that release after process death may be delayed while system resources are reclaimed; an immediate restart may therefore fail once rather than risk breaking a live lock.
- Every lock error names the directory. Contention says it is already open, while unsupported targets additionally name the missing locking capability.
- Ownership is asserted before anything is touched, and `Open` then deletes the store's own temporary files left by a crashed predecessor. It removes only regular files bearing the two temporary prefixes: not directories, not links, not installed files, not `LOCK`, not unrelated names. Exclusive ownership is what makes that deletion safe to do unconditionally.
- Ownership ends at `Close`, but the in-memory contents do not. Reads keep working against a detached, stale view after the lock is released, so a caller shutting down does not have to race its own readers. That view can never observe a later owner of the directory, and every mutation returns `ErrClosed`.
