# `context.Context` appears only on `Open`

`Get`, `Set`, `Delete`, and their variants take no context, which will look wrong to anyone used to threading one through every call. `Get` never performs I/O, and `Set` blocks only on a queue whose depth the caller configured, so a context would decorate calls that have nothing to cancel — and cancelling a `Set` that has already been handed to the writer cannot un-write it.

## Consequences

- Cancellation is meaningful exactly where the store does unbounded work: recovery, which `Open` bounds with the context it is given.
- The data path stays allocation-free of context values and readable without asking what a cancelled read would even mean.
