# HTTP Streaming Sample

An Azure Function that streams a server-sent events (SSE) response over HTTP.

## What this sample demonstrates

- Forcing the host's HTTP-streaming path by calling `http.Flusher` after each chunk. The host's `HttpUri` capability forwards the request directly to the worker's loopback listener so chunked transfer / `text/event-stream` / `http.Flusher.Flush()` work natively.
- Using `slog.InfoContext(r.Context(), ...)` (not stdlib `log.Println`) so each record correlates to the invocation via trace.id / span.id and structured attrs (invocation_id, function_name, trigger_type) when `middleware/otelfunc` is in use.
- `span.SetAttributes(...)` on the worker invocation span (reachable via `trace.SpanFromContext(r.Context())`) is auto-harvested by `middleware/otelfunc` and forwarded to the host's parent AspNetCore activity. Works the same on streaming as on gRPC-body — the user-facing API doesn't change between transports.

## Prerequisites

- [Go 1.24+](https://go.dev/dl/)
- Custom [Azure Functions Core Tools](https://www.npmjs.com/package/@gaaguiar/azure-functions-core-tools) with Go worker support:
  ```bash
  npm i -g @gaaguiar/azure-functions-core-tools
  ```

## Setup

```bash
cd samples/httpStreaming
go mod init myapp
go get github.com/azure/azure-functions-golang-worker
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
