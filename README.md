# go-systems

A collection of small Go programs, each a standalone systems component.

| Directory | Project |
| --- | --- |
| [`cache/`](cache/) | In-memory key-value store with TTL and a write-ahead log |
| [`proxy/`](proxy/) | HTTP reverse proxy and load balancer |
| [`resp/`](resp/) | Redis-compatible TCP server |
| [`queue/`](queue/) | Distributed job queue over gRPC |
| [`sysmon/`](sysmon/) | Terminal process and system monitor |

Each directory has its own README. Projects are independent; pick whichever you care about.

Nothing is implemented yet. Code will land in those directories as each project is built.
