# queue

Distributed job queue: producers dispatch work, workers execute it.

Typed tasks travel over gRPC bidirectional streams. Workers pull jobs, run them, retry on failure, and report status. Slow workers apply backpressure instead of letting queues grow without bound.

## Features (planned)

- Protobuf + gRPC streaming
- Worker pool with retries
- Status updates back to the producer
- Backpressure on a slow consumer

## Status

Not implemented.

## Requirements

- Go (version TBD when a `go.mod` is added)
- Protocol Buffers / gRPC toolchain (when the API is added)

## Build

```bash
go build ./...
```

## Test

```bash
go test -race ./...
```
