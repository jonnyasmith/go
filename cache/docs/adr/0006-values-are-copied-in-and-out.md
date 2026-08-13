# The store copies values on the way in and on the way out

`Set` copies the caller's slice and `Get` returns a copy, at the cost of an allocation on every read. Handing out the stored slice would be zero-copy, and a single caller mutating it would silently corrupt the store for every other reader — a failure with no stack trace and no way for the store to detect it.

## Consequences

- Read throughput carries an allocation and the resulting GC pressure. This is the accepted price of an API that cannot be corrupted by a well-meaning caller.
- `GetInto` lets a caller supply its own buffer and avoid the allocation, which is the supported path for hot loops.
- This interacts with capacity accounting: the bytes charged against capacity are the store's own copy, and a caller's copy is its own problem. If read allocation ever dominates a benchmark, the answer is to move that caller to `GetInto` — never to hand out the stored slice. The copy is a correctness property, not a tuning parameter.
