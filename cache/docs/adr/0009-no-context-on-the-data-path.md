# `context.Context` appears only on `Open`

`Get`, `Set`, `Delete`, and their variants take no context, which will look wrong to anyone used to threading one through every call. `Get` never performs I/O. A mutation may wait for the writer and the filesystem, but once its request has been published to the writer, cancellation cannot reverse it or truthfully report that it did not happen. The call therefore waits for the definitive write result instead of offering an ambiguous cancellation result.

## Consequences

- Cancellation is meaningful during recovery because `Open` can stop before a `Store` is exposed and before any mutation is accepted.
- Callers always receive the definitive outcome of a mutation. A shorter-lived caller may stop waiting in its own goroutine, but the mutation retains one owner and completes exactly once.
- "`Get` never performs I/O" is an argument about cancellation, not a claim that reads are side-effect free. A read takes its shard's lock, drops the entry if its deadline has passed, and updates recency and counters. It cannot block on a disk, which is the only property the decision rests on.
