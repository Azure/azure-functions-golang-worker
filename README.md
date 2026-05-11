# Azure Functions Go Worker

[![Go Report Card](https://goreportcard.com/badge/github.com/Azure/azure-functions-golang-worker)](https://goreportcard.com/report/github.com/Azure/azure-functions-golang-worker)
[![Go Reference](https://pkg.go.dev/badge/github.com/Azure/azure-functions-golang-worker.svg)](https://pkg.go.dev/github.com/Azure/azure-functions-golang-worker)

The `azure-functions-golang-worker` repository provides the SDK and worker implementation to run Go (Golang) applications natively on Azure Functions. This allows developers to write serverless applications using familiar idiomatic Go structures, such as standard `net/http` handlers and structured types, while deeply integrating with Azure Function's bindings and trigger ecosystem.

## Features

* **Native Go Feel:** Use standard `http.ResponseWriter` and `*http.Request` for HTTP APIs.
* **Worker-driven Indexing:** No need to manually author `function.json` files. Define your triggers and bindings directly in Go code using a fluent builder API.
* **First-Class Performance:** Runs in an out-of-process model utilizing gRPC for highly performant bidirectional communication with the Azure Functions host.
* **Rich Bindings:** Built-in reflection to map Azure bindings (like blobs, queues, CosmosDB) into strictly typed Go pointers and structs.

---

## Getting Started

### Prerequisites

* [Go 1.24+](https://go.dev/dl/)
* Custom [Azure Functions Core Tools](https://www.npmjs.com/package/@gaaguiar/azure-functions-core-tools) with Go worker support

```bash
# Install the specialized core tools to run locally
npm i -g @gaaguiar/azure-functions-core-tools
```

### Writing Your First Function

Initialize a standard Go module for your project:

```bash
mkdir my-go-func
cd my-go-func
go mod init myapp
go get github.com/azure/azure-functions-golang-worker
go mod tidy
```

Create a `main.go` file:

```go
package main

import (
"fmt"
"net/http"

"github.com/azure/azure-functions-golang-worker/sdk"
"github.com/azure/azure-functions-golang-worker/worker"
)

func main() {
app := sdk.FunctionApp()

// Register an HTTP trigger using familiar types
app.HTTP("hello", hello).Methods("GET", "POST").Auth("anonymous")

// Start the worker
worker.Start(app)
}

func hello(w http.ResponseWriter, r *http.Request) {
name := r.URL.Query().Get("name")
if name == "" {
name = "Azure"
}
fmt.Fprintf(w, "Hello, %s! Welcome to Go on Azure Functions.", name)
}
```

Run the application locally using the Core Tools:

```bash
func start
```
*Note: `func start` will automatically compile and build your Go application before starting the local Azure Functions host.*

### Examples & Samples

For more advanced scenarios involving Azure storage blobs, events grids, Cosmos DB, and testing strategies, please visit the [samples/](samples/) directory.

---

## Trigger Model: Core Triggers vs Extension Triggers

The Go Worker organizes triggers into two tiers based on their dependency requirements:

### Core Triggers (`sdk/`)

**HTTP, Timer, CosmosDB, ServiceBus, EventHub, EventGrid**

These triggers receive data inline via gRPC — the host serializes the trigger payload (JSON documents, messages, events) into the `InvocationRequest` and the worker deserializes it into typed Go structs. They have:

- **Typed handler signatures** — e.g., `func(context.Context, []bindings.CosmosDocument) error`
- **Zero external dependencies** — only `encoding/json` needed for deserialization
- **Bounded payloads** — change feed docs, queue messages, and events are discrete, size-limited objects

```go
// Core trigger — typed, no extra imports needed
app.CosmosDB("processChanges", handler).
    Database("mydb").Container("mycontainer").Connection("CosmosDBConnection")
```

### Extension Triggers (`triggers/`)

**Blob** (and future Queue, Table, etc.)

These triggers provide an authenticated Azure SDK client instead of raw data. The host sends only metadata (container, blob path); the worker constructs a client scoped to the specific resource. They have:

- **SDK client injection** — handler receives e.g., `*blob.Client` ready to use
- **Isolated dependencies** — `azblob`, `azidentity` etc. live in `triggers/blob/`, activated via blank import
- **Streaming support** — user can `DownloadStream()` without buffering GBs through gRPC

```go
import _ "github.com/azure/azure-functions-golang-worker/triggers/blob" // activate extension

// Extension trigger — handler gets a live SDK client
app.Blob("processBlobTrigger", handler).
    Path("samples-workitems/{name}").Connection("AzureWebJobsStorage")
```

### When is each tier used?

| Criterion | Core (data passthrough) | Extension (SDK client) |
|---|---|---|
| Payload size | Bounded (KB–low MB) | Potentially unbounded (GBs) |
| External SDK needed? | No | Yes |
| Data in gRPC message? | Yes — already serialized by host | No — only metadata |
| Streaming? | Not needed | Essential |
| Handler type | Typed alias (`CosmosDBHandler`) | `any` (validated via reflection) |

This is similar to the .NET worker extensions model (`Microsoft.Azure.Functions.Worker.Extensions.*`) but avoids over-abstracting core triggers that don't need external dependencies.

---

## Custom Handlers vs First-Class Go Worker

Historically, Go was only supported on Azure Functions via "Custom Handlers" (an HTTP-based proxy pattern). This new natively supported Go Worker provides a richer experience:
1. **gRPC Integration**: The worker connects directly to the host process via an `EventStream`, reducing HTTP proxy overhead.
2. **First-class Bindings**: You no longer need to parse raw HTTP headers to read trigger/binding data; the gRPC worker deserializes the metadata and binding data directly into your Go objects.
3. **No configurations:** Function endpoints are discovered cleanly in code without `function.json`.

---

## Telemetry & Observability

The worker emits structured logs and distributed traces with minimal setup.

### Structured logging

The SDK installs an [`slog`](https://pkg.go.dev/log/slog) handler at package init that routes every record over the gRPC log channel back to the host. Each entry automatically carries `invocation_id`, `function_name`, and `trigger_type`, so logs in Application Insights are correlated to the right invocation without any user wiring:

```go
slog.InfoContext(ctx, "processing item", "item_id", id, "size_bytes", n)
```

The default handler honors the host's per-category log levels and the `--verbose` flag. Call `slog.SetDefault` yourself if you need a different backend.

### OpenTelemetry distributed tracing

The [`middleware/otelfunc`](middleware/otelfunc) package provides an `sdk.Middleware` that creates a server-kind span around every invocation, extracts the host's W3C trace context so user spans correlate end-to-end, advertises the `WorkerOpenTelemetryEnabled` capability so the host stops double-emitting telemetry, and force-flushes after each invocation (critical on consumption-style plans where the worker may be frozen).

The middleware is **opt-in**: importing only `sdk` and `worker` keeps the OTel SDK out of your binary entirely. The smallest setup just registers the middleware and sets the standard OTel env vars on your Function App:

```go
import (
    "github.com/azure/azure-functions-golang-worker/middleware/otelfunc"
)

app := sdk.FunctionApp()
app.Use(otelfunc.Middleware())
```

```
OTEL_EXPORTER_OTLP_ENDPOINT=https://otlp.your-backend.example
OTEL_EXPORTER_OTLP_HEADERS=api-key=<your_token>
OTEL_SERVICE_NAME=my-function-app
```

For more control, build the exporters yourself and pass them as options. `WithExporter` and `WithLogExporter` can be called multiple times to fan out to several backends:

```go
import (
    "github.com/azure/azure-functions-golang-worker/middleware/otelfunc"
    "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
    "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
)

otlpExp, _ := otlptracehttp.New(ctx)
debugExp, _ := stdouttrace.New(stdouttrace.WithWriter(os.Stderr))

app := sdk.FunctionApp()
app.Use(otelfunc.Middleware(
    otelfunc.WithExporter(otlpExp),
    otelfunc.WithExporter(debugExp),
    otelfunc.WithResource(
        semconv.ServiceVersion(buildVersion),
        semconv.DeploymentEnvironmentName("production"),
    ),
))
```

Inbound W3C baggage is hydrated onto `ctx` automatically — read with `baggage.FromContext(ctx)`, propagate to your downstream calls with `otelhttp.NewTransport(...)` / `otelgrpc` interceptors. See `package otelfunc` godoc for full options including `WithTracerProvider`, `WithPropagator`, custom span names, and the `AZURE_FUNCTIONS_WORKER_OPENTELEMETRY_DISABLED` kill switch.

For a deeper architectural overview see the [developer manual](TECHNICAL_SPEC.md).

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft trademarks or logos is subject to and must follow [Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general). Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship. Any use of third-party trademarks or logos are subject to those third-party's policies.
