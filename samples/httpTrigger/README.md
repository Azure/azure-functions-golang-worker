# HTTP Trigger Sample

An Azure Function with an HTTP trigger that responds to GET and POST requests.

## What this sample demonstrates

- The idiomatic Go HTTP handler signature (`func(w http.ResponseWriter, r *http.Request)`) used directly as a Functions trigger handler — no SDK-specific types.
- Reading per-invocation metadata via [`sdk.FromContext(r.Context())`](../../sdk/context.go) — `InvocationContext` is attached to the standard `*http.Request` context.
- Emitting structured logs with [`slog.InfoContext`](https://pkg.go.dev/log/slog) that automatically carry `invocation_id`, `function_name`, and `trigger_type`.
- A small in-line [`sdk.Middleware`](../../sdk/middleware.go) example (`timingMiddleware`) that measures invocation duration and logs the result. The same middleware seam is how `middleware/otelfunc` plugs in distributed tracing.

## Prerequisites

- [Go 1.24+](https://go.dev/dl/)
- [Azure Functions Core Tools](https://www.npmjs.com/package/azure-functions-core-tools/v/4.12.0) 4.12.0 or later (includes Go worker support):
  ```bash
  npm i -g azure-functions-core-tools@4 --unsafe-perm true
  ```

## Setup

```bash
cd samples/httpTrigger
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
curl http://localhost:7071/api/hello
```

Expected response: `Hello from Go Worker!`
