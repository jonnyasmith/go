# resp

TCP server that speaks the Redis Serialization Protocol (RESP2/3).

Clients such as `redis-cli` connect over a raw socket. The server reads framed commands, executes them, and writes RESP replies. No HTTP, no extra protocol layer.

## Features (planned)

- RESP2/3 over `net.Listener`
- Compatible with `redis-cli`
- Streaming parse and encode
- Pipelined commands

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

## Usage

Once the server is running, point a Redis client at it:

```bash
redis-cli -p <port> PING
```
