# Collector → Azure Monitor sample

Demonstrates forwarding **both host and worker** telemetry to **Azure Monitor** through an OpenTelemetry Collector running **inside** the Azure Functions Go worker process. It uses the [`otelcollector`](../../otelcollector/README.md) package for the embedded collector and [`otelfunc`](../../middleware/otelfunc/README.md) to produce the worker telemetry.

The entire integration is one line:

```go
worker.Start(app, otelcollector.WithCollector())
```

`WithCollector()` starts the collector (listening on `localhost:4317`/`4318`) before the worker serves, gates on readiness, and flushes/shuts it down on teardown.

## Telemetry flow

```
worker (otelfunc) ─┐
                   ├─ OTLP/HTTP → localhost:4318 → embedded collector → Azure Monitor DCE
Functions host ────┘
```

Both the worker and the host export to the collector via `OTEL_EXPORTER_OTLP_ENDPOINT` using OTLP/HTTP (`OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`, matching the collector's `:4318` HTTP receiver). The collector authenticates to Azure Monitor with managed identity and forwards traces, logs, and metrics to the Data Collection Endpoint.

## Configuration

This sample ships **no** `otel-collector-config.yaml` — it relies on the `otelcollector` package's compiled-in default config, which already forwards traces, logs, and metrics to an Azure Monitor Data Collection Endpoint. You only need to supply the DCE endpoints (and OTLP transport) as app settings / `local.settings.json` values:

```
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
OTEL_DCE_TRACES_ENDPOINT=<dce>/.../traces
OTEL_DCE_LOGS_ENDPOINT=<dce>/.../logs
OTEL_DCE_METRICS_ENDPOINT=<dce>/.../metrics
```

To customize the pipeline, drop an `otel-collector-config.yaml` next to the binary (file-next-to-executable precedence) or use `otelcollector.WithConfigFile(...)` / `WithConfigYAML(...)`.

`host.json` sets `"telemetryMode": "OpenTelemetry"`.

## Run

```
func start
curl http://localhost:7071/api/hello
```

Spans, logs, and metrics for the invocation appear in the Azure Monitor resource backing the DCE.
