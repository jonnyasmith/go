# Cache Instructions

## Routing

- Cache vocabulary → docs/agents/domain.md
- Cache decisions → docs/adr/
- Byte-level layout of segments, snapshots, and recovery → docs/on-disk-format.md

## Verification

- MUST run `go fmt ./...` before verification.
- MUST run `go vet ./...`.
- MUST run `go test -race -count=1 ./...`.
- MUST run `govulncheck ./...` before merging.
- SHOULD use native Go fuzzing and benchmarks for relevant changes.
- MUST NOT add a task runner.
- MUST keep development tools outside `cache/go.mod`.
