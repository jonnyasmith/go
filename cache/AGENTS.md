# Cache Instructions

## Routing

- Cache vocabulary → docs/agents/domain.md
- Cache decisions, including the on-disk format and its evolution → docs/adr/

## Verification

- MUST run `go fmt ./...` before verification.
- MUST run `go vet ./...`.
- MUST run `go test -race -count=1 ./...`.
- MUST run `govulncheck ./...` before merging.
- SHOULD use native Go fuzzing and benchmarks for relevant changes.
- MUST NOT add a task runner.
- MUST keep development tools outside `cache/go.mod`.
