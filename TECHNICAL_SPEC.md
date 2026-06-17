# Azure Functions Go Worker - Technical Specification

## 1. Introduction

The Azure Functions Go Worker enables developers to write Azure Functions in Go (Golang) with a first-class experience. Unlike previous iterations or the custom handler approach, this worker integrates deeply with the Azure Functions Host via gRPC, offering rich binding support, worker-driven function indexing, and a familiar Go programming model.

## 2. Architecture

### 2.1 Overview

The host launches the user's compiled Go binary (`app`) directly. The binary contains the Function Worker SDK, the user's function code, and SDK bindings. The host communicates with it over a bidirectional gRPC stream using the `FunctionRpc` protocol.

### 2.2 Components

1. **The Host**: The Azure Functions Host (Runtime), which manages triggers and orchestrates invocations via gRPC.
2. **The Worker (`app` / `app.exe`)**: The user's compiled Go application. It contains the Function Worker SDK, the user's function code, and SDK bindings. The worker's logging pipeline lives in [`worker/log/`](worker/log/doc.go): a bootstrap stderr handler used pre-stream, a Writer that emits `RpcLog` values on the established gRPC stream, User-/System-category slog handlers, and a process-global Observer registry that middleware (notably `middleware/otelfunc`) uses to bridge user log records into other sinks without the worker importing their dependencies.

### 2.3 Communication Protocol

The Worker uses the gRPC `FunctionRpc.EventStream` bidirectional streaming RPC. The message lifecycle is:

1. **StartStream** — Worker sends its `WorkerId` to establish the stream.
2. **WorkerInitRequest / WorkerInitResponse** — Host initializes the worker; worker reports capabilities and version.
3. **FunctionsMetadataRequest / FunctionMetadataResponse** — Host requests function metadata; worker responds with all registered functions (worker-driven indexing via `"workerIndexing": "true"` in `worker.config.json`).
4. **FunctionLoadRequest / FunctionLoadResponse** — Host loads individual functions by ID.
5. **InvocationRequest / InvocationResponse** — Host invokes functions; worker executes and returns results.
6. **WorkerStatusRequest / WorkerStatusResponse** — Health checks.
7. **FunctionEnvironmentReloadRequest** — Environment variable reload (used during specialization in placeholder mode).
8. **WorkerTerminate** — Graceful shutdown signal.

Startup arguments are passed via command-line flags:

```
--functions-uri <gRPC address>
--functions-worker-id <worker ID>
--functions-request-id <request ID>
--functions-grpc-max-message-length <max bytes>
```

### 2.4 Startup Flow

The host reads `worker.config.json`, finds `defaultExecutablePath`, and launches the user's binary directly. The flow is:

```
Host  ──gRPC──>  app (user binary)
```

The user binary:
1. Parses startup args (`worker.GetWorkerStartupConfig()`).
2. Connects to the host's gRPC server (`connectToHost()`).
3. Sends `StartStream`.
4. Enters the main message loop (`handleBidiStream()`), dispatching each message through the `Dispatcher`.

## 3. User Application Structure

### 3.1 File Structure

A Go Function App is a standard Go module. With worker-driven indexing, there are no `function.json` files — functions are registered in code.

```text
my-function-app/
├── host.json
├── local.settings.json
├── go.mod
├── go.sum
└── main.go               <-- Entry point
```

### 3.2 `main.go`

The user's `main.go` imports the SDK, registers functions using functional options, and starts the worker:

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
    app.HTTP("hello", hello,
        sdk.WithMethods("GET", "POST"),
        sdk.WithAuth("anonymous"),
    )
    worker.Start(app)
}

func hello(w http.ResponseWriter, r *http.Request) {
    name := r.URL.Query().Get("name")
    if name == "" {
        name = "world"
    }
    fmt.Fprintf(w, "Hello, %s!", name)
}
```

HTTP trigger handlers use standard `net/http` types (`http.ResponseWriter`, `*http.Request`), making Go Functions feel native.

### 3.3 Functional Options API

Functions are registered using the functional options pattern. Each trigger type has a registration method on `App` that accepts variadic `Option` functions:

```go
// HTTP trigger
app.HTTP("name", handler,
    sdk.WithMethods("GET", "POST"),
    sdk.WithAuth("anonymous"),
)

// Blob trigger (via triggers/blob module)
app.Blob("processBlobTrigger", handler,
    sdk.WithPath("samples-workitems/{name}"),
    sdk.WithConnection("AzureWebJobsStorage"),
)

// CosmosDB trigger
app.CosmosDB("processChanges", handler,
    sdk.WithDatabase("mydb"),
    sdk.WithContainer("mycontainer"),
    sdk.WithConnection("CosmosDBConnection"),
)
```

### 3.4 Worker-Driven Indexing

The Go Worker uses worker-driven function indexing (`"workerIndexing": "true"` in `worker.config.json`). This means:

- The host does **not** scan the filesystem for `function.json` files.
- Instead, it sends a `FunctionsMetadataRequest` to the worker.
- The worker responds with metadata for all registered functions, including binding info, generated from the in-code registrations.
- Function IDs are generated by hashing the function name.

## 4. Trigger Architecture: Core Triggers vs Extension Triggers

The Go Worker organizes triggers into two tiers. The split is driven by a concrete
dependency boundary, not by convention alone.

### 4.1 Core Triggers — Data Passthrough (`sdk/`)

**HTTP, Timer, CosmosDB, ServiceBus, EventHub, EventGrid, SQL**

Core triggers receive their payload inline in the gRPC `InvocationRequest`. The
host serializes the trigger data (JSON documents, queue messages, timer info) into
the protobuf message, and the worker deserializes it into typed Go structs via
`FromProto()` → `convertToTypeValue()` → `json.Unmarshal`.

**Why these are core:**
- Payloads are bounded (change feed batches, individual messages, timer metadata)
- Deserialization requires only `encoding/json` — zero external Azure SDK deps
- Re-fetching the data from the service would be wasteful (extra latency, RU/API cost)
- Typed handler aliases (e.g., `CosmosDBHandler`, `TimerHandler`) provide compile-time safety

Core trigger types are defined in `sdk/handlers.go` and binding structs in `sdk/bindings/`.

```go
// Core trigger — typed handler, no extra imports
type CosmosDBHandler = func(context.Context, []bindings.CosmosDocument) error

app.CosmosDB("processChanges", handler,
    sdk.WithDatabase("mydb"),
    sdk.WithContainer("items"),
    sdk.WithConnection("CosmosDBConnection"),
)
```

### 4.2 Extension Triggers — SDK Client Injection (`triggers/`)

**Blob** (and future Queue, Table, etc.)

Extension triggers provide an authenticated Azure SDK client scoped to the specific
resource that triggered the function. The host sends only metadata (container name,
blob path); the blob content never flows through gRPC.

**Why these are extensions:**
- Payloads are potentially unbounded (blobs can be GBs)
- A useful handler needs an Azure SDK client (`azblob`, `azidentity`) — heavy deps
- Streaming access (`DownloadStream()`) avoids buffering entire payloads in memory
- Isolating these deps in `triggers/<name>/` keeps binaries small for users who don't need them

Extension triggers use the `ClientFactory` registry pattern (see `sdk/factories.go`).
The extension package registers a factory via `init()`, following the same pattern
as `database/sql` driver registration.

### 4.3 ClientFactory Mechanism

1. **User Import**: The user blank-imports the extension package:
   `import _ "github.com/azure/azure-functions-golang-worker/triggers/blob"`
2. **Registration**: The extension's `init()` calls `sdk.RegisterClientFactory("blobTrigger", factory)`
   to register a `ClientFactory` for the trigger type.
3. **Invocation**:
   - Worker receives an `InvocationRequest`.
   - It extracts the binding config (path, connection) and trigger metadata.
   - It calls the registered `ClientFactory` with this config.
   - The factory creates and returns an authenticated SDK client (e.g., `*blob.Client`).
   - The client replaces the trigger argument in the handler's argument list.
4. **Result**: The handler receives a ready-to-use SDK client — no manual auth or connection setup.

The handler type for extension triggers is `any` (validated at registration time via
reflection) rather than a typed alias, because the concrete client type varies by
extension.

### 4.4 Decision Criteria

| Criterion | Core (data passthrough) | Extension (SDK client) |
|---|---|---|
| Payload size | Bounded (KB–low MB) | Potentially unbounded (GBs) |
| External Azure SDK needed? | No | Yes |
| Data in gRPC message? | Yes — already serialized | No — only metadata |
| Streaming? | Not needed | Essential |
| Handler type | Typed alias (compile-time safe) | `any` (reflection-validated) |
| Location | `sdk/handlers.go`, `sdk/bindings/` | `triggers/<name>/` |

This is analogous to the .NET Isolated worker extensions model
(`Microsoft.Azure.Functions.Worker.Extensions.*`), but avoids over-abstracting core
triggers that don't need isolated dependencies.

### 4.5 Example: Blob Extension Trigger

```go
package main

import (
    "context"
    "fmt"
    "io"
    "log"

    "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
    "github.com/azure/azure-functions-golang-worker/sdk"
    _ "github.com/azure/azure-functions-golang-worker/triggers/blob" // registers ClientFactory
    "github.com/azure/azure-functions-golang-worker/worker"
)

func main() {
    app := sdk.FunctionApp()
    app.Blob("processBlobTrigger", processBlob,
        sdk.WithPath("samples-workitems/{name}"),
        sdk.WithConnection("AzureWebJobsStorage"),
    )
    worker.Start(app)
}

func processBlob(ctx context.Context, client *blob.Client) error {
    // client is already authenticated and scoped to the triggering blob
    get, err := client.DownloadStream(ctx, nil)
    if err != nil {
        return fmt.Errorf("download error: %w", err)
    }
    data, _ := io.ReadAll(get.Body)
    get.Body.Close()
    log.Printf("Blob size: %d bytes", len(data))
    return nil
}
```

### 4.6 Example: Core Trigger (CosmosDB)

```go
package main

import (
    "context"
    "log"

    "github.com/azure/azure-functions-golang-worker/sdk"
    "github.com/azure/azure-functions-golang-worker/sdk/bindings"
    "github.com/azure/azure-functions-golang-worker/worker"
)

func main() {
    app := sdk.FunctionApp()
    app.CosmosDB("processChanges", handler,
        sdk.WithDatabase("mydb"),
        sdk.WithContainer("items"),
        sdk.WithConnection("CosmosDBConnection"),
    )
    worker.Start(app)
}

func handler(ctx context.Context, docs []bindings.CosmosDocument) error {
    // docs are deserialized directly from the gRPC message — no SDK client needed
    for _, doc := range docs {
        log.Printf("Document: %s", doc.ID)
    }
    return nil
}
```

<a id="sql-trigger"></a>

### 4.7 Example: Core Trigger (SQL)

The SQL trigger is a Core trigger backed by the
`Microsoft.Azure.WebJobs.Extensions.Sql` host extension. The host polls SQL
Server / Azure SQL Change Tracking for committed row changes and delivers
each batch inline as `[]bindings.SQLChange`. The worker has no SQL or
`mssql` dependency — the payload is serialized JSON.

```go
package main

import (
    "context"
    "encoding/json"
    "log/slog"

    "github.com/azure/azure-functions-golang-worker/sdk"
    "github.com/azure/azure-functions-golang-worker/sdk/bindings"
    "github.com/azure/azure-functions-golang-worker/worker"
)

type Product struct {
    ProductID int    `json:"ProductId"`
    Name      string `json:"Name"`
    Cost      int    `json:"Cost"`
}

func productsChanged(ctx context.Context, changes []bindings.SQLChange) error {
    for _, c := range changes {
        var p Product
        if err := json.Unmarshal(c.Item, &p); err != nil {
            return err
        }
        slog.InfoContext(ctx, "row change",
            "operation", c.Operation.String(),
            "product_id", p.ProductID)
    }
    return nil
}

func main() {
    app := sdk.FunctionApp()
    app.SQL("productsChanged", productsChanged,
        sdk.WithTable("dbo.Products"),
        sdk.WithConnection("AzureWebJobsSqlConnectionString"),
    )
    worker.Start(app)
}
```

See `samples/sqlTrigger/` for a runnable end-to-end example, including the
exact `ALTER` statements and `host.json` logging configuration needed for
the host extension's startup banner to surface in logs.

## 5. Per-Invocation Context, Middleware, and Observability

Three intertwined concepts shape how user code interacts with the worker beyond plain trigger handlers: the **per-invocation context**, the **middleware seam**, and **structured logging**. They are designed together — solving each in isolation would have produced a clunkier surface — and the bundled `middleware/otelfunc` package is a regular consumer of all three.

### 5.1 `InvocationContext` on `context.Context`

For every invocation the dispatcher builds an `sdk.InvocationContext` from the inbound `pb.InvocationRequest` and attaches it to `context.Context` via `sdk.NewContext(parent, ic)`:

```go
type InvocationContext struct {
    InvocationID    string
    FunctionID      string
    FunctionName    string
    TriggerType     string             // "httpTrigger", "timerTrigger", ...
    TraceContext    TraceContext       // W3C trace-parent / state / baggage
    RetryContext    RetryContext       // RetryCount, MaxRetryCount
    TriggerMetadata map[string]string  // host-supplied trigger metadata
}
```

User code retrieves it with `sdk.FromContext(ctx)`:

```go
func TimerHandler(ctx context.Context, timer bindings.TimerInfo) error {
    ic, _ := sdk.FromContext(ctx)
    if ic != nil && ic.RetryContext.RetryCount > 0 {
        // running on a retry attempt
    }
    return nil
}
```

HTTP handlers reach the same context via `r.Context()` — the dispatcher attaches the per-invocation context to the `*http.Request` it passes to the handler, so the standard `net/http` signature stays unchanged and `sdk.FromContext(r.Context())` works exactly like the timer case above.

Tagging the host's parent AspNetCore activity from inside a handler goes through the worker invocation span. Calls to `span.SetAttributes(...)` on the span obtained via `trace.SpanFromContext(ctx)` are auto-harvested by `middleware/otelfunc` at end-of-invocation, recorded on a framework-only `sdk.MiddlewareContext` (a wrapper that embeds the user-facing `*InvocationContext` and carries cross-cutting state the dispatcher reads when building the response), and forwarded on `InvocationResponse.TraceContextAttributes`. The host then applies each entry as a tag on its parent activity via `Activity.AddTag(k, v)`. Works identically on the gRPC-body and HTTP-streaming paths, so a Flusher / SSE handler tags the host span the same way an `r.Body`-reading handler does. Matches the dotnet-isolated worker's `Activity.Tags`-harvest behavior.

The choice of `context.Context` as the carrier (rather than a struct parameter) means handler signatures stay short, middleware can enrich the context with span objects / baggage / cancellation, and OpenTelemetry SDKs that read trace context from `context.Context` plug in transparently.

### 5.2 Middleware

The worker exposes a minimal interface and a function-based adapter:

```go
type Handler func(ctx context.Context, ic *InvocationContext) error

type Middleware interface {
    Wrap(next Handler) Handler
}

type MiddlewareFunc func(next Handler) Handler // adapter for plain-function middleware
```

Users register middleware via `App.Use(mw)`. Registration order matches the standard Go convention: the first-registered middleware is outermost and runs first on entry / last on exit. `App.Compose(inner)` walks the registered slice in reverse to produce the wrapped chain — used by the worker dispatcher for both the gRPC-body invocation path and the HTTP-streaming proxy path, so middleware wraps every trigger type uniformly.

Two optional contracts let middleware participate in worker-level lifecycle events without making them mandatory:

```go
type CapabilityProvider interface {
    Capabilities() map[string]string
}

type ShutdownProvider interface {
    Shutdown(ctx context.Context) error
}
```

When a registered middleware implements `CapabilityProvider`, the App merges its capability map into `WorkerInitResponse.Capabilities` so the host knows what features the worker supports (most importantly `WorkerOpenTelemetryEnabled` from `middleware/otelfunc`, which tells the host to suppress forwarding of worker-emitted `Function.*` log records into the host's own OpenTelemetry log pipeline — the worker is expected to emit those itself via its own LoggerProvider). When a middleware implements `ShutdownProvider`, the worker invokes its `Shutdown(ctx)` after the gRPC stream closes or on SIGTERM/SIGINT — so middleware that own asynchronous resources (OTel batch processors, exporters) get flushed without user code needing a `defer cleanup()` line.

#### 5.2.1 Example: timing middleware

```go
func timingMiddleware(next sdk.Handler) sdk.Handler {
    return func(ctx context.Context, ic *sdk.InvocationContext) error {
        start := time.Now()
        err := next(ctx, ic)
        slog.InfoContext(ctx, "invocation finished",
            "duration_ms", time.Since(start).Milliseconds(),
            "err", err,
        )
        return err
    }
}

func main() {
    app := sdk.FunctionApp()
    app.Use(sdk.MiddlewareFunc(timingMiddleware))
    app.HTTP("hello", HTTPTriggerHandler, sdk.WithMethods("GET"))
    worker.Start(app)
}
```

### 5.3 Structured logging via `slog`

The SDK installs an `slog` handler at package init that routes every record through the gRPC stream as a typed `RpcLog` (replacing the legacy stderr `log.Printf` path). User log records flow on the same channel as the worker's own diagnostics but with distinct categories so the host filters them correctly.

Key behaviors:

- **Automatic invocation attrs**: records emitted with a context (e.g. `slog.InfoContext(ctx, ...)`) carry `invocation_id`, `function_name`, and `trigger_type` attributes pulled from `sdk.FromContext(ctx)` without user wiring.
- **Typed properties**: structured attributes land in `RpcLog.PropertiesMap` — the typed field the host forwards to Application Insights `customDimensions`. The same attributes are also rendered into `RpcLog.Message` in logfmt format so they remain visible in backends that ignore properties.
- **Host-driven filtering**: the dispatcher applies the `log_categories` map sent in `WorkerInitRequest` so per-category log-level overrides from `host.json` take effect. When no filter is installed (cold-start window), records default-allow so early logs aren't silently dropped.
- **Bootstrap buffering**: records emitted before the gRPC stream is open (arg parsing, dial errors) are buffered by `worker.bootstrap_logger` and flushed onto the gRPC channel once the stream is established.
- **System vs User**: a separate System-category `slog.Logger` is constructed for worker-internal events (dispatcher startup, message dispatch, function load). It bypasses the SDK wrapper so it doesn't pick up user invocation attrs.

```go
slog.InfoContext(ctx, "processing order",
    "order_id", id,
    "size_bytes", len(payload),
)
// Record arrives at the host with severityLevel=Information,
// properties {order_id, size_bytes, invocation_id, function_name, trigger_type},
// and a message string listing every attribute.
```

### 5.4 The user log observer hook

`worker/log.RegisterObserver(fn log.Observer)` is a package-level extension point that lets third-party packages observe every user log record without the worker importing their dependencies. The worker's user log handler fans every record out to every registered observer **after** the RpcLog has been enqueued on the outbound stream, so the host-facing log path is the source of truth and observer failures (network errors, slow exporters) don't derail user invocations.

The contract is intentionally narrow:

- Observers are called synchronously in the goroutine that emitted the record.
- Bound attrs (accumulated via `slog.With(...)`) are replayed onto the cloned record passed to observers, so each observer sees the same structure the RpcLog received without re-implementing attr accumulation.
- Registration is process-global and append-only; observers can't be removed (so `sync.Once`-guarded registration is the right pattern, as `middleware/otelfunc` uses).

The motivating consumer is the `middleware/otelfunc` package: it registers an observer that bridges user `slog` records into the configured OpenTelemetry `LoggerProvider`. Crucially, the worker itself contains no OpenTelemetry imports — users who never `import middleware/otelfunc` get zero OTel packages compiled into their binary (verified: 17.87 MB vs 21.25 MB).

### 5.5 `middleware/otelfunc` — OpenTelemetry distributed tracing

`middleware/otelfunc.Middleware()` is the canonical example of how the middleware seam, capability advertising, shutdown lifecycle, and the log observer hook compose into a complete cross-cutting concern. From the user's perspective the simplest setup is:

```go
import "github.com/azure/azure-functions-golang-worker/middleware/otelfunc"

app := sdk.FunctionApp()
app.Use(otelfunc.Middleware())
```

…combined with the standard OpenTelemetry environment variables on the Function App (`OTEL_EXPORTER_OTLP_ENDPOINT`, optionally `OTEL_EXPORTER_OTLP_HEADERS` and `OTEL_SERVICE_NAME`).

What the middleware does on every invocation:

1. Extracts inbound W3C trace context from `RpcTraceContext.TraceParent` / `TraceState` and attaches it to `ctx` via the configured propagator, so worker spans correlate with the host's parent activity.
2. Hydrates inbound baggage onto `ctx` via `baggage.ContextWithBaggage`, so user code reading `baggage.FromContext(ctx)` sees upstream-supplied baggage members.
3. Starts an internal-kind span named `function <FunctionName>` carrying `faas.invocation_id`, `faas.name`, `faas.trigger`, `process.pid`, `faas.instance`, and `azure.functions.live_logs_session_id` semconv / Azure-specific attributes.
4. Calls `next(ctx, ic)` — the user handler runs with the enriched context.
5. Records any returned error on the span and sets the span status to Error.
6. `ForceFlush`es the owned TracerProvider and LoggerProvider so telemetry is pushed before the host can freeze the container between invocations on consumption-style plans.

Implementation details — provider resolution priority, the auto-OTLP path that wins over the OpenTelemetry global to dodge the `go.opentelemetry.io/auto/sdk` IsRecording wrapper, multi-backend fan-out via stackable `WithExporter`, and the kill switch — live in the package godoc (`middleware/otelfunc`).

## 6. Deployment

### 6.1 Compilation

The user's code must be compiled for the target platform:

```bash
# For Linux (Azure Functions default)
GOOS=linux GOARCH=amd64 go build -o app .

# For Windows
GOOS=windows GOARCH=amd64 go build -o app.exe .

# For local development (current OS)
go build -o app .    # or app.exe on Windows
```

### 6.2 Deployment Artifacts

#### Dedicated Mode (Local Development / Dedicated Plan)

The deployment artifact contains:

1. `app` (the compiled user binary)
2. `worker.config.json`
3. `host.json`

#### Consumption Mode (with Placeholder Support)

The deployment artifact contains:

1. `app` (the compiled user binary)
2. `proxy` (the platform interface)
3. `worker.config.json` (points `defaultExecutablePath` to the proxy)
4. `host.json`

### 6.3 `worker.config.json`

For dedicated/local development, `defaultExecutablePath` points to the user binary:

```json
{
    "description": {
        "language": "golang",
        "defaultExecutablePath": "{AzureWebJobsScriptRoot}/app",
        "workerIndexing": "true"
    },
    "processOptions": {
        "initializationTimeout": "00:02:00",
        "environmentReloadTimeout": "00:02:00"
    }
}
```

For consumption with proxy, `defaultExecutablePath` points to the proxy:

```json
{
    "description": {
        "language": "golang",
        "defaultExecutablePath": "proxy",
        "workerIndexing": "true"
    },
    "processOptions": {
        "initializationTimeout": "00:02:00",
        "environmentReloadTimeout": "00:02:00"
    }
}
```

## 7. Local Development with Core Tools

### 7.1 `func init --worker-runtime golang`

Initializes a new Go Function App project, generating:

- `host.json` with extension bundle configuration
- `local.settings.json` with `FUNCTIONS_WORKER_RUNTIME` set to `"native"`
- `main.go` with a sample HTTP trigger function
- `go.mod` via `go mod init`

### 7.2 `func start`

Core tools:
1. Invoked as `func start --worker-runtime go` to select the Go worker.
2. Runs `go build -o app .` (or `app.exe` on Windows) to compile the project.
3. Starts the Azure Functions Host, which reads `worker.config.json` and launches `app`.
4. The host and worker communicate over gRPC.
5. HTTP trigger endpoints are displayed (e.g., `http://localhost:7071/api/hello`).

### 7.3 Docker Support

`func init --worker-runtime golang --docker` generates a `Dockerfile`:

```dockerfile
FROM mcr.microsoft.com/azure-functions/dotnet:4

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

COPY . /home/site/wwwroot
```

The user pre-compiles their binary for Linux before building the Docker image.
