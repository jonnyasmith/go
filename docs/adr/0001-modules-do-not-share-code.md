# Modules are independent and never import each other

Each of the five modules is a self-contained Go module with its own `go.mod`, wired together only by a root `go.work` so the repository still builds as a whole. Overlap between them is expected: the cache is a library with no network listener even though a Redis-compatible server lives in `resp/`, and `resp/` will hold its own storage rather than depending on `cache/`.

## Consequences

- Each module can be copied out of the repository and used on its own.
- Duplication across modules is not a defect to be factored into a shared package; a shared package would couple release and design decisions that are deliberately separate.
