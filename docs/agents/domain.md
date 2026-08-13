# Domain vocabulary

What you need in order to steer work on this solution. Each module README states the problem and the Go internals that module exercises.

| Domain | What to understand | Where it shows up |
| --- | --- | --- |
| Goroutine scheduler (GMP) | How M OS threads map to G goroutines via P logical processors; why blocking I/O does not stall OS threads | `proxy/`, `queue/` |
| Channels vs mutexes | Share memory by communicating (channels) vs communicate by sharing memory (`RWMutex`, `atomic`) | `cache/`, `queue/` |
| Memory allocation | Stack vs heap, escape analysis, GC pause times | `cache/`, `resp/` |
| Streaming and interfaces | `io.Reader` / `io.Writer` as the I/O backbone, without inheritance | `resp/` |
| Context cancellation | Tree-like cancellation: timeouts, deadlines, and signals across calls | `cache/`, `proxy/` |
