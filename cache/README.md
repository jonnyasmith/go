# cache

In-memory key-value store with TTL eviction and disk persistence.

The map lives in memory. Writes go to an append-only write-ahead log first. A background worker expires keys. On restart, the store is rebuilt from the log (and optional snapshots). Shutdown drains in-flight work, flushes, then exits.

## Features (planned)

- Concurrent get/set/delete
- Configurable TTL and active cleanup
- Snapshots for faster recovery
- Append-only WAL
- Graceful shutdown on SIGINT / SIGTERM

## Status

Not implemented.

## Requirements

- Go (version TBD when a `go.mod` is added)

## Build

```bash
go build ./...
```

## Test

```bash
go test -race ./...
```
