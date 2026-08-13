# sysmon

Terminal process and system monitor for Linux.

Reads kernel metrics from `/proc` (and `unix` syscalls where `/proc` is not enough), walks process trees, and shows CPU and memory in a TUI.

## Features (planned)

- Live process tree
- Per-process CPU and memory
- Host-level stats from `/proc`
- Interactive TUI ([Bubble Tea](https://github.com/charmbracelet/bubbletea))

## Status

Not implemented.

## Requirements

- Linux
- Go (version TBD when a `go.mod` is added)

## Build

```bash
go build ./...
```

## Test

```bash
go test -race ./...
```

## Run

```bash
go run .
```
