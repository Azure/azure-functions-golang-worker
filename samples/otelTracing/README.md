# OpenTelemetry tracing sample

The smallest possible end-to-end OpenTelemetry setup for the Go worker.

## What this sample shows

```go
func main() {
    app := sdk.FunctionApp()
    app.Use(otelfunc.Middleware())
    app.HTTP("hello", HelloHandler, sdk.WithMethods("GET"), sdk.WithAuth("anonymous"))
    worker.Start(app)
}
```

`otelfunc.Middleware()` is zero-config when the standard `OTEL_*` environment variables are set. It:

- Creates an internal-kind span named `function hello` around every invocation, with `faas.invocation_id` / `faas.name` / `faas.trigger` attributes (matches the dotnet-isolated and Java workers).
- Extracts the host's W3C trace context from `RpcTraceContext` so worker spans correlate with the host's parent AspNetCore span.
- Bridges `slog` records into the OTel `LoggerProvider`, so logs carry `trace.id` / `span.id` automatically and land in the same backend as the traces.
- Auto-harvests any `span.SetAttributes(...)` calls from inside the handler onto `InvocationResponse.TraceContextAttributes`, which the host applies to its parent span via `Activity.AddTag`. Tag the worker invocation span with values resolved at handler time (`tenant_id`, `user_id`, etc.) and they appear on the host's "requests" record too.
- Force-flushes both providers after every invocation — important on consumption-style plans where the host may freeze the worker between invocations.
- Shuts down owned providers cleanly when the gRPC stream closes or the process receives SIGTERM/SIGINT.

## Configure

Edit `local.settings.json` (for local dev) or set the same variables in the Function App's app settings (for deployed runs):

```
OTEL_EXPORTER_OTLP_ENDPOINT=https://otlp.your-backend.example
OTEL_EXPORTER_OTLP_HEADERS=api-key=<your_api_key>
OTEL_SERVICE_NAME=my-go-function-app
```

Backend-specific examples:

- **New Relic**: `OTEL_EXPORTER_OTLP_ENDPOINT=https://otlp.nr-data.net`, `OTEL_EXPORTER_OTLP_HEADERS=api-key=<your_nr_license_key>`
- **Datadog**: point at your Datadog agent's OTLP endpoint, e.g. `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318`
- **Azure Monitor**: requires the Azure Monitor exporter, not OTLP. See [`middleware/otelfunc`](../../middleware/otelfunc/README.md) for the `WithExporter` pattern.

## Prerequisites

- Go 1.25+
- Azure Functions Core Tools with Go worker support

## Run locally

```bash
cd samples/otelTracing
go mod init otel-tracing-sample
go get github.com/azure/azure-functions-golang-worker
go mod tidy
go build -o app .
func start
```

Hit the endpoint:

```bash
curl http://localhost:7071/api/hello
```

In your OTel backend you should see:

1. A trace containing the host's AspNetCore server span (`GET api/hello`) as the parent.
2. The worker's `function hello` internal span as its child.
3. Log records correlated to the worker span via `trace.id` / `span.id`.

## Advanced configuration

For multiple exporters, custom Resource attributes, custom propagators, the kill switch, or shutdown lifecycle questions, see the [`middleware/otelfunc` README](../../middleware/otelfunc/README.md).
