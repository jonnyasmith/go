# The cache depends on the standard library alone

No third-party module appears in `cache/go.mod`, for the runtime or for tests. The obvious pull is a metrics library, and it is refused: `Stats` returns a plain struct read from atomic counters and the caller exports it however its deployment wants, rather than the store dictating a metrics ecosystem to every program that embeds it.

## Consequences

- The store carries no transitive dependency risk and no version pressure from anything it embeds into.
- There is no metrics interface for callers to implement either, since an indirect call on every hit would cost more than the counter it feeds.
- Tests use `testing` and its fuzzing support only; crash injection is a subprocess and a signal, not a framework.
- The rule binds `go.mod` only, not the toolchain. `gofmt`, `go vet`, and `go test -race` are the gate, and a pinned external linter is fine because it never enters the module graph.
