# proxy

HTTP/HTTPS reverse proxy and load balancer.

Incoming requests are routed to a backend, with health checks, rate limiting, and a choice of balancing strategy. Client deadlines travel with the request so a hung origin cannot hold a goroutine forever.

## Features (planned)

- Dynamic routing
- Active and passive health checks
- Rate limiting (token bucket / leaky bucket)
- Round-robin and least-connections balancing
- HTTPS termination

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
