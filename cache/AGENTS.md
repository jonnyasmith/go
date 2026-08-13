# Cache Instructions

## Routing — read only what the task needs, when it needs it

### This context

- Cache vocabulary → docs/agents/domain.md
- Cache decisions → docs/adr/
- Byte-level layout of segments, snapshots, and recovery → docs/on-disk-format.md

## Tooling

Use the Go toolchain directly; do not add a task runner.

Before verification:

```sh
go fmt ./...
```

Required verification:

```sh
go vet ./...
go test -race -count=1 ./...
```

Use native Go tooling for targeted work:

```sh
go test ./...
go test -fuzz=<target> -fuzztime=<duration>
go test -bench=. -benchmem ./...
```

Before merging, scan reachable code for known vulnerabilities with the official Go vulnerability scanner:

```sh
govulncheck ./...
```

Install or update the scanner outside the module dependency graph:

```sh
go install golang.org/x/vuln/cmd/govulncheck@latest
```
