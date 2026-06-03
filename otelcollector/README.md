# `otelcollector` — Embedded OpenTelemetry Collector for the Azure Functions Go worker

A top-level package that runs an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) **inside the worker process** and manages its lifecycle for you. It pairs with [`middleware/otelfunc`](../middleware/otelfunc/README.md): `otelfunc` produces OTLP telemetry from the worker, and `otelcollector` receives it on `localhost` and forwards it to your backend (Azure Monitor by default).

Like `otelfunc`, this package is **opt-in**: apps that never import it compile no collector code into their binary.

## Why

Without this package, embedding a collector means hand-writing factory wiring, config resolution, a run goroutine, readiness gating, and graceful shutdown in every app's `main()`. `otelcollector` owns all of that and exposes a single option on `worker.Start`.

## Quickstart

```go
package main

import (
    "log/slog"
    "net/http"

    "github.com/azure/azure-functions-golang-worker/middleware/otelfunc"
    "github.com/azure/azure-functions-golang-worker/otelcollector"
    "github.com/azure/azure-functions-golang-worker/sdk"
    "github.com/azure/azure-functions-golang-worker/worker"
)

func hello(w http.ResponseWriter, r *http.Request) {
    slog.InfoContext(r.Context(), "hello", "path", r.URL.Path)
    w.Write([]byte("hi"))
}

func main() {
    app := sdk.FunctionApp()
    app.Use(otelfunc.Middleware()) // export OTLP to the embedded collector
    app.HTTP("hello", hello, sdk.WithMethods("GET"))

    // Starts the collector before serving; flushes and shuts it down on teardown.
    worker.Start(app, otelcollector.WithCollector())
}
```

The collector listens for OTLP on `localhost:4317` (gRPC) and `localhost:4318` (HTTP). Point both the worker and the Functions host at it:

```
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
```

`host.json` must set `"telemetryMode": "OpenTelemetry"` (see the `otelfunc` README).

## How it integrates

`worker.Start` accepts `sdk.StartOption` values. `WithCollector` returns one that registers a lifecycle hook:

- **Before serving:** the collector is built, configured, started, and the call blocks until it reaches the running state (or the start timeout elapses).
- **On teardown:** the collector is gracefully shut down (flushing buffered telemetry) alongside middleware shutdowns.

The `worker` package never imports `otelcollector` — the bridge is the neutral [`sdk.LifecycleHook`](../sdk/lifecycle.go) interface, so apps that don't use the collector pull in none of its dependencies.

## Configuration

The collector configuration is resolved from the first available source (the env confmap provider is always enabled, so `${env:...}` references expand regardless of source):

1. Inline YAML via `WithConfigYAML(...)`
2. A file via `WithConfigFile(...)`
3. `otel-collector-config.yaml` next to the executable
4. The compiled-in default (`DefaultConfigYAML`)

The default config forwards traces, logs, and metrics to an Azure Monitor [Data Collection Endpoint](https://learn.microsoft.com/azure/azure-monitor/essentials/data-collection-endpoint-overview) using managed-identity auth, reading three endpoints from the environment:

```
OTEL_DCE_TRACES_ENDPOINT
OTEL_DCE_LOGS_ENDPOINT
OTEL_DCE_METRICS_ENDPOINT
```

Azure Monitor expects DELTA metric temporality, so the metrics pipeline includes the `cumulativetodelta` processor.

For details on the OTLP endpoints, stream names, and DCR/DCE setup that Azure Monitor requires, see [OpenTelemetry protocol (OTLP) ingestion](https://learn.microsoft.com/en-us/azure/azure-monitor/containers/opentelemetry-protocol-ingestion).

## Options

| Option | Purpose |
|---|---|
| `WithConfigFile(path)` | Load config from a file. |
| `WithConfigYAML(yaml)` | Load config from an inline string (highest precedence). |
| `WithFactories(f)` | Replace the bundled component factory set. Use `DefaultFactories()` as a base to add custom components. |
| `FailFast()` | Make a startup failure terminate the worker (default: degrade with a warning and continue). |
| `StartTimeout(d)` | Override the readiness wait (default: 10s). |
| `WithBuildInfo(bi)` | Override the collector's reported BuildInfo. |

## Bundled components

`DefaultFactories()` includes:

- **receivers:** `otlp`
- **processors:** `batch`, `cumulativetodelta`
- **exporters:** `otlphttp`
- **extensions:** `azureauth` (Azure Monitor managed-identity auth)

## Advanced use

Add custom components by extending the bundled factories:

```go
f, _ := otelcollector.DefaultFactories()
f.Exporters[myexp.NewFactory().Type()] = myexp.NewFactory()

worker.Start(app, otelcollector.WithCollector(
    otelcollector.WithFactories(f),
    otelcollector.WithConfigFile("/path/to/config.yaml"),
))
```

Or drive the collector directly and operate on the underlying `*otelcol.Collector`:

```go
col, err := otelcollector.Start(ctx, otelcollector.WithConfigYAML(cfg))
if err != nil { /* ... */ }
defer col.Shutdown(context.Background())
raw := col.Unwrap() // *otelcol.Collector
```

See [`samples/collectorToAzureMonitor`](../samples/collectorToAzureMonitor) for a complete, deployable example.
