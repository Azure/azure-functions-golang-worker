# `middleware/otelfunc` — OpenTelemetry middleware for Azure Functions Go worker

A drop-in [`sdk.Middleware`](../../sdk/middleware.go) that adds distributed tracing and structured-log → OTel-log bridging to the Azure Functions Go worker. Designed to be **opt-in**: users who never import this package get zero OpenTelemetry packages compiled into their binary.

## Quickstart (env-var only)

The simplest setup. Set the standard OpenTelemetry environment variables on the Function App, register the middleware, you're done.

```go
package main

import (
    "log/slog"
    "net/http"

    "github.com/azure/azure-functions-golang-worker/middleware/otelfunc"
    "github.com/azure/azure-functions-golang-worker/sdk"
    "github.com/azure/azure-functions-golang-worker/worker"
)

func helloHandler(w http.ResponseWriter, r *http.Request) {
    slog.InfoContext(r.Context(), "hello", "path", r.URL.Path)
    w.Write([]byte("hi"))
}

func main() {
    app := sdk.FunctionApp()
    app.Use(otelfunc.Middleware()) // zero config; reads OTEL_EXPORTER_OTLP_* from env
    app.HTTP("hello", helloHandler, sdk.WithMethods("GET"))
    worker.Start(app)
}
```

App settings:

```
OTEL_EXPORTER_OTLP_ENDPOINT=https://otlp.your-backend.example
OTEL_EXPORTER_OTLP_HEADERS=api-key=<token>
OTEL_SERVICE_NAME=my-function-app
```

That's it. Every invocation gets an internal-kind span (named `function <FunctionName>`) correlated with the host's parent AspNetCore activity, every `slog` call inside the handler carries `trace.id`/`span.id`, and both traces and logs are force-flushed between invocations so consumption-style plans don't lose buffered batches when the container is frozen.

## What you get on every invocation

| Behavior | Notes |
|---|---|
| Internal-kind span named `function <FunctionName>` | The host's AspNetCore instrumentation owns the SERVER-kind span for HTTP triggers; non-HTTP triggers don't represent a network server. Matches the Java worker. Override the formatter with [`WithSpanNameFormatter`](middleware.go). |
| W3C `traceparent`/`tracestate` extracted from `RpcTraceContext` | Worker spans correlate with host parent activity |
| Inbound W3C baggage hydrated onto `ctx` | Read with `baggage.FromContext(ctx)` |
| Standard `faas.invocation_id`, `faas.name`, `faas.trigger` semconv attrs | Custom static attrs via [`WithAttributes`](middleware.go) |
| Per-invocation span attrs `process.pid`, `faas.instance`, `azure.functions.live_logs_session_id` | Promoted from host-supplied `RpcTraceContext.Attributes`; matches the Java worker. Live-logs session ID is portal-only. |
| Default Resource: `cloud.provider=azure`, `cloud.platform=azure_functions`, `cloud.region`, `cloud.resource_id`, `deployment.environment.name`, `service.name`, `otel.library.version` | `cloud.resource_id` is omitted on Flex Consumption (the platform doesn't inject `WEBSITE_RESOURCE_GROUP`; the .NET host's `FunctionsResourceDetector` has the same gap). Customers can supply it via `OTEL_RESOURCE_ATTRIBUTES` or [`WithResource`](middleware.go). |
| User span attributes auto-harvested onto the host's parent AspNetCore span | `span.SetAttributes(...)` calls inside the handler are read at end-of-invocation, filtered to drop worker-set semconv keys (`faas.*`, `process.pid`, `faas.instance`, `azure.functions.live_logs_session_id`), and forwarded on `InvocationResponse.TraceContextAttributes`. Host applies via `Activity.AddTag`. Works on gRPC-body and HTTP-streaming paths. Matches the dotnet-isolated worker. |
| User `slog` records bridged to the OTel `LoggerProvider` | Same `trace.id`/`span.id` as the worker span |
| `ForceFlush` after every invocation (both TracerProvider and LoggerProvider) | Critical for Flex/Consumption plans |
| Graceful shutdown via `sdk.ShutdownProvider` | Worker invokes on stream close / SIGTERM / SIGINT |
| Capability advertising (`WorkerOpenTelemetryEnabled=true`) | Tells host to suppress its own forwarding of worker `Function.*` log records (worker emits via its own LoggerProvider). Host AspNetCore HTTP-server spans and `Host.*` traces are not affected. |

## Configuration options

| Option | Stackable | Purpose |
|---|:---:|---|
| `WithTracerProvider(tp)` | | Bring your own. Highest priority. Takes ownership lifecycle (we do not call Shutdown on it). |
| `WithLoggerProvider(lp)` | | Same for the log path. |
| `WithExporter(e)` | ✅ | Use one or more SpanExporters; the middleware builds an owned TracerProvider with a default Resource. |
| `WithLogExporter(e)` | ✅ | Same for log exporters. |
| `WithResource(...attrs)` | ✅ | Extend the default Resource with typed attributes (`service.version`, `deployment.environment`, build SHA). |
| `WithPropagator(p)` | | Override the default `TraceContext+Baggage` propagator composite. |
| `WithFlusher(f)` / `WithoutFlusher()` | | Customize or disable the per-invocation force-flush. |
| `WithSpanNameFormatter(fn)` | | Custom span naming rule. Default returns `ic.FunctionName`. |
| `WithAttributes(...attrs)` | ✅ | Extra attributes on the invocation span (not the Resource — those go to `WithResource`). |

### Provider resolution priority

When no explicit `WithTracerProvider` is supplied, the middleware tries to build or acquire a TracerProvider in this order:

1. **`WithExporter(e)`** — build owned TracerProvider around the supplied exporters.
2. **`OTEL_EXPORTER_OTLP_ENDPOINT`** — auto-build an OTLP HTTP TracerProvider with the default Resource.
3. **`otel.GetTracerProvider()`** — fall back to the OTel global if non-noop.

The same order applies to `LoggerProvider` (with `WithLogExporter` / `OTEL_EXPORTER_OTLP_*` / `global.GetLoggerProvider()`).

**Why auto-OTLP wins over the global**: the OTel global TracerProvider is a delegating wrapper that routes spans through `go.opentelemetry.io/auto/sdk`, which is pulled in transitively by `otelslog` and several contrib bridges. That auto SDK reports spans as `IsRecording=true` for eBPF-agent capture, so a behavioral noop check returns false even when nothing is wired up. Treating the wrapper as "user configured" and skipping auto-OTLP would silently swallow every span when the user only set the env var. So when both an env var AND a non-noop-looking global are present, the env var wins.

### Multi-backend fan-out

`WithExporter` and `WithLogExporter` accept multiple calls — each registered exporter gets its own batch processor on the same owned provider:

```go
otlpExp, _ := otlptracehttp.New(ctx)
debugExp, _ := stdouttrace.New(stdouttrace.WithWriter(os.Stderr))

app.Use(otelfunc.Middleware(
    otelfunc.WithExporter(otlpExp),
    otelfunc.WithExporter(debugExp),
))
```

For routing without changing user code (e.g. send to App Insights AND a third-party backend simultaneously), point `OTEL_EXPORTER_OTLP_ENDPOINT` at a sidecar [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) and configure fan-out exporters there.

### Kill switch

Setting `AZURE_FUNCTIONS_WORKER_OPENTELEMETRY_DISABLED=true` makes the middleware a pass-through without redeploying — useful for triage and per-slot rollouts. The middleware stops creating spans, stops advertising the OTel capability, and the host resumes its own Application Insights emission.

## Logs ↔ traces correlation

The middleware registers a [`log.Observer`](../../worker/log/user.go) (exactly once per process, guarded by `sync.Once`) that bridges every user `slog` record into the OpenTelemetry `LoggerProvider` via the `otelslog` contrib bridge.

The result: user log records emitted during an invocation carry the same `trace.id` and `span.id` as the worker span the middleware created. In most OTel backends this surfaces as clickable trace correlation on log records, plus the ability to filter logs by trace.

```go
func helloHandler(w http.ResponseWriter, r *http.Request) {
    // Both records carry trace.id and span.id pointing at the
    // otelfunc-created server span for this invocation.
    slog.InfoContext(r.Context(), "received request", "path", r.URL.Path)

    // Same correlation extends to child spans started in user code.
    _, span := otel.Tracer("my-app").Start(r.Context(), "lookup")
    defer span.End()
    slog.InfoContext(r.Context(), "lookup running")
}
```

## Configuration for common backends

### Application Insights via an OpenTelemetry Collector sidecar

There's no first-party Go exporter for Azure Monitor / Application Insights. The recommended path is to send OTLP to a [Collector](https://opentelemetry.io/docs/collector/) that fans out to the `azuremonitor` exporter.

Worker side:

```
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector.internal:4318
```

Collector `config.yaml`:

```yaml
receivers:
  otlp:
    protocols:
      http: { endpoint: 0.0.0.0:4318 }

exporters:
  azuremonitor:
    connection_string: ${env:APPLICATIONINSIGHTS_CONNECTION_STRING}

service:
  pipelines:
    traces:  { receivers: [otlp], exporters: [azuremonitor] }
    logs:    { receivers: [otlp], exporters: [azuremonitor] }
```

### Direct OTLP to any backend

Most managed observability vendors expose OTLP/HTTP ingestion endpoints — the env vars above are all you need.

### Explicit exporters

When you want full control (custom processor, sampler, or a non-OTLP exporter), construct the providers in code and pass them via `WithTracerProvider` / `WithLoggerProvider`. The middleware will use them as-is and will *not* shut them down on worker termination (the caller owns the lifecycle).

## Going further

- Full API surface and design notes: `go doc github.com/azure/azure-functions-golang-worker/middleware/otelfunc`
- Architecture overview: [`TECHNICAL_SPEC.md`](../../TECHNICAL_SPEC.md) section 5
- Worked examples: [`example_test.go`](example_test.go)

## How it stays opt-in

The worker package itself contains **zero** imports from `go.opentelemetry.io/*`. The user log observer hook ([`worker/log.RegisterObserver`](../../worker/log/user.go)) is the only integration seam, and registration happens inside this package's `Middleware()` constructor. Users who never import `middleware/otelfunc` get zero OTel packages compiled into their final binary — measured on a minimal HTTP-trigger app:

| Configuration | OTel packages compiled in | Binary size |
|---|---:|---:|
| `sdk` + `worker` only | **0** | 17.87 MB |
| `+ app.Use(otelfunc.Middleware())` | 61 | 21.25 MB |
