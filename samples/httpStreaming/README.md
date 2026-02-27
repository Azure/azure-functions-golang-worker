# HTTP Streaming Sample

An Azure Function that streams a server-sent events (SSE) response over HTTP.

## Prerequisites

- [Go 1.24+](https://go.dev/dl/)
- [Azure Functions Core Tools](https://github.com/Azure/azure-functions-core-tools) with Go worker support

## Setup

```bash
cd samples/httpStreaming
go mod init myapp
go mod edit -require github.com/azure/azure-functions-golang-worker@v0.0.0
go mod edit -replace github.com/azure/azure-functions-golang-worker=../..
go mod tidy
```

## Run

```bash
func start
```

`func start` automatically builds the Go project before launching. To skip the build step (e.g., if you've already built manually), use:

```bash
func start --no-build
```

## Test

```bash
curl http://localhost:7071/api/stream
```

Expected response (streamed over ~5 seconds):

```
data: event 0

data: event 1

data: event 2

data: event 3

data: event 4
```
