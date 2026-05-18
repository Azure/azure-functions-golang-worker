# Middleware sample

Demonstrates the two styles for defining middleware in the Go worker and the registration-order semantics of `app.Use`.

## What this sample shows

### Two definition styles

**Function-based** (stateless, the common case) — wrap a `func(next sdk.Handler) sdk.Handler` in `sdk.MiddlewareFunc`. Same pattern as `net/http`'s `HandlerFunc`:

```go
func timingMiddleware(next sdk.Handler) sdk.Handler {
    return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
        start := time.Now()
        err := next(ctx, mc)
        slog.InfoContext(ctx, "invocation finished",
            "duration_ms", time.Since(start).Milliseconds())
        return err
    }
}

app.Use(sdk.MiddlewareFunc(timingMiddleware))
```

**Struct-based** (when per-instance state is needed) — declare a type with a `Wrap(next sdk.Handler) sdk.Handler` method. The struct's fields become the middleware's state:

```go
type invocationCounter struct {
    count atomic.Int64
}

func (c *invocationCounter) Wrap(next sdk.Handler) sdk.Handler {
    return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
        c.count.Add(1)
        return next(ctx, mc)
    }
}

app.Use(&invocationCounter{})
```

### Registration ordering

`app.Use` follows the standard Go convention (chi, gin, echo, net/http chains): **first registered is outermost**. For middlewares registered in order `A`, `B`, `C`, every invocation runs:

```
A.before → B.before → C.before → handler → C.after → B.after → A.after
```

The sample registers three ordering-demo middlewares (`A`, `B`, `C`) that each log on entry and exit, so you can see the bracketing in the log output.

### About `sdk.MiddlewareContext`

The second parameter of a Handler is `*sdk.MiddlewareContext`. It embeds `*sdk.InvocationContext`, so trigger-side fields are reachable directly via field promotion:

```go
mc.FunctionName   // same as mc.InvocationContext.FunctionName
mc.InvocationID   // same as mc.InvocationContext.InvocationID
mc.TraceContext   // same as mc.InvocationContext.TraceContext
```

Most middleware code that only reads invocation metadata uses these promoted fields and never needs to think about the wrapper. The wrapper exposes framework-only methods like `SetOutboundTraceAttribute` for middleware that needs to write framework state the worker dispatcher reads when building the response.

User HTTP handlers receive the simpler `*sdk.InvocationContext` via `sdk.FromContext(r.Context())`; the `MiddlewareContext` is the middleware-author surface only.

## Prerequisites

- Go 1.25+
- Azure Functions Core Tools with Go worker support

## Run locally

```bash
cd samples/middleware
go mod init middleware-sample
go get github.com/azure/azure-functions-golang-worker
go mod tidy
go build -o app .
func start
```

Then hit the endpoint:

```bash
curl http://localhost:7071/api/hello
```

The function output is uninteresting (the sample is the middleware chain, not the handler). The log output is the point — observe the bracketed ordering of the `A`/`B`/`C` middlewares around the handler, plus the timing record and the per-invocation counter.
