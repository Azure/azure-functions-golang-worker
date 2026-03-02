# Azure Functions Go Worker - Technical Specification

## 1. Introduction

The Azure Functions Go Worker enables developers to write Azure Functions in Go (Golang) with a first-class experience. Unlike previous iterations or the custom handler approach, this worker integrates deeply with the Azure Functions Host via gRPC, offering rich binding support, worker-driven function indexing, and a familiar Go programming model.

## 2. Architecture

### 2.1 Overview

The Go Worker architecture supports two distinct deployment modes to address Go's static compilation nature:

- **Dedicated / Local Development** — The host launches the user's compiled Go binary (`app`) directly. No proxy is needed.
- **Consumption / Placeholder** — A lightweight Proxy process handles the placeholder lifecycle, then spawns the user binary on specialization.

### 2.2 Components

1. **The Host**: The Azure Functions Host (Runtime), which manages triggers and orchestrates invocations via gRPC.
2. **The Worker (`app` / `app.exe`)**: The user's compiled Go application. It contains the Function Worker SDK, the user's function code, and SDK bindings. The host communicates with it over a bidirectional gRPC stream using the `FunctionRpc` protocol.
3. **The Proxy (`proxy` / `proxy.exe`)**: A lightweight Go process used only in consumption/placeholder deployments. It brokers gRPC communication between the host and the user's binary.

### 2.3 Communication Protocol

Both the Worker and the Proxy use the same gRPC `FunctionRpc.EventStream` bidirectional streaming RPC. The message lifecycle is:

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

### 2.4 Dedicated Mode (Local Development)

In dedicated mode — including local development with `func start` — the host reads `worker.config.json`, finds `defaultExecutablePath`, and launches the user's binary directly. The flow is:

```
Host  ──gRPC──>  app (user binary)
```

The user binary:
1. Parses startup args (`worker.GetWorkerStartupConfig()`).
2. Connects to the host's gRPC server (`connectToHost()`).
3. Sends `StartStream`.
4. Enters the main message loop (`handleBidiStream()`), dispatching each message through the `Dispatcher`.

No proxy is involved. This is the simplest path and what `func start` uses.

### 2.5 Placeholder Mode (Consumption Plan)

The Azure Functions platform uses "Placeholder" workers to reduce cold start latency. Placeholders are pre-warmed containers initialized before a specific user application is assigned to them. When the platform assigns ("specializes") a placeholder to a user app, it sends a `FunctionEnvironmentReloadRequest` containing the user's environment variables and application directory.

#### 2.5.1 Architectural Constraints

Unlike managed runtimes (.NET, Java, Python) which can dynamically load user code (DLLs, JARs, `.py` files) into a running process, Go compiles into static, self-contained binaries. A Go binary bundles the runtime, garbage collector, and user logic into a single inseparable unit. This prevents the Go Worker from adopting the "code injection" specialization strategy used by .NET Isolated (FunctionsNetHost) or the module-loading approach used by Python/Node.

#### 2.5.2 Strategy: The Proxy Model

To support specialization without incurring a full cold start penalty (container creation + network setup), the Go Worker implements a Parent/Child Proxy Model.

**The Placeholder Phase (Parent Process):**

1. The platform starts `proxy` as the entry point in the Docker image.
2. The Proxy connects to the host's gRPC server and sends `StartStream`.
3. It starts a local gRPC server on a random port (`127.0.0.1:0`) to accept connections from the child worker.
4. It handles messages from the host with stub responses:
   - `WorkerInitRequest` → Responds with success and a set of worker capabilities. Saves the request for later replay to the child.
   - `FunctionsMetadataRequest` → Returns an empty function list (no user code loaded yet).
   - `WorkerHeartbeat` → Echoes back.
   - `WorkerStatusRequest` → Responds with empty status.
5. The container stays "warm" — network, gRPC connection, and process are all established.

**Specialization Event:**

When the host sends `FunctionEnvironmentReloadRequest`:

1. The Proxy marks itself as specializing (prevents double-specialization).
2. It prepares the child process environment by merging the host-provided environment variables with the current environment.
3. It determines the user binary path: `{FunctionAppDirectory}/app` (or `app.exe` on Windows).
4. It spawns the user binary as a child process, pointing it at the Proxy's local gRPC server (not the host's):
   ```
   app --functions-uri http://127.0.0.1:<proxy-port> \
       --functions-worker-id <same-worker-id> \
       --functions-request-id <same-request-id> \
       --functions-grpc-max-message-length <same-max-length>
   ```
5. The child connects to the Proxy's local gRPC server via `EventStream`.

**Message Bridging:**

Once the child connects:

1. The Proxy replays the saved `WorkerInitRequest` to the child.
2. The child's `StartStream` is dropped (the Proxy already sent its own to the host).
3. The child's `WorkerInitResponse` is dropped in placeholder mode (the Proxy already responded to the host).
4. All subsequent messages from the host are forwarded to the child.
5. All subsequent messages from the child are forwarded to the host.

The Proxy becomes an opaque bidirectional pipe:

```
Host  ──gRPC──>  Proxy  ──local gRPC──>  app (user binary)
      <──gRPC──         <──local gRPC──
```

**Dedicated Mode via Proxy:**

If `WEBSITE_PLACEHOLDER_MODE` is not set to `"1"`, the Proxy immediately spawns the child worker without waiting for a `FunctionEnvironmentReloadRequest`. In this case, the child's `WorkerInitResponse` is forwarded to the host (since the Proxy did not pre-respond).

## 3. User Application Structure

### 3.1 File Structure

A Go Function App is a standard Go module. With worker-driven indexing, there are no `function.json` files — functions are registered in code.

```text
my-function-app/
├── host.json
├── local.settings.json
├── worker.config.json
├── go.mod
├── go.sum
└── main.go               <-- Entry point
```

### 3.2 `main.go`

The user's `main.go` imports the SDK, registers functions using the fluent builder API, and starts the worker:

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
    app.HTTP("hello", hello).Methods("GET", "POST").Auth("anonymous")
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

### 3.3 Builder API

Functions are registered using a fluent builder pattern:

```go
// HTTP trigger
app.HTTP("name", handler).Methods("GET", "POST").Auth("anonymous")

// HTTP trigger with blob input binding
app.HTTP("process", handler).
    Methods("POST").
    Auth("function").
    BlobInput("input", "container/{id}", "AzureWebJobsStorage")

// Blob trigger
app.Blob("processBlobTrigger", handler).
    Path("samples-workitems/{name}").
    Connection("AzureWebJobsStorage")

// CosmosDB trigger
app.CosmosDB("processChanges", handler).
    Database("mydb").
    Container("mycontainer").
    Connection("CosmosDBConnection")
```

### 3.4 Worker-Driven Indexing

The Go Worker uses worker-driven function indexing (`"workerIndexing": "true"` in `worker.config.json`). This means:

- The host does **not** scan the filesystem for `function.json` files.
- Instead, it sends a `FunctionsMetadataRequest` to the worker.
- The worker responds with metadata for all registered functions, including binding info, generated from the in-code registrations.
- Function IDs are generated by hashing the function name.

## 4. Rich SDK Bindings (Registry Pattern)

To support Azure SDK types (e.g., `*azblob.Client`, `*blob.Client`) directly in function signatures, the worker implements a **Registry Pattern**.

### 4.1 Concept

The core Worker is agnostic of specific Azure SDKs. It parses binding configurations into a generic map. Separate "Extension" packages register **Converters** that know how to transform binding configuration + trigger metadata into usable SDK client objects.

### 4.2 Implementation

1. **User Import**: The user imports the extension package (e.g., `_ "github.com/azure/azure-functions-golang-worker/sdk/extensions/blob"`). The blank import triggers the package's `init()` function.
2. **Registration**: The extension's `init()` function calls `sdk.RegisterConverter()` to register factory functions for specific Go types with the global registry.
3. **Invocation**:
   - Worker receives an `InvocationRequest`.
   - During `FromProto()`, it identifies the desired type in the user's handler signature (e.g., `*blob.Client`).
   - It queries the Registry (`sdk.GetConverter()`) for a converter matching that type.
   - The Converter uses binding configuration (e.g., connection string name from the binding, blob path from trigger metadata) to create and return the Azure SDK client.
4. **Result**: The user's handler receives a fully-initialized Azure SDK client — no manual connection setup needed.

### 4.3 Example: Blob Extension

```go
package main

import (
    "log"

    "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
    "github.com/azure/azure-functions-golang-worker/sdk"
    _ "github.com/azure/azure-functions-golang-worker/sdk/extensions/blob" // registers converter
    "github.com/azure/azure-functions-golang-worker/worker"
)

func main() {
    app := sdk.FunctionApp()
    app.Blob("processBlobTrigger", processBlob).
        Path("samples-workitems/{name}").
        Connection("AzureWebJobsStorage")
    worker.Start(app)
}

func processBlob(client *blob.Client) {
    // client is already connected to the specific blob that triggered the function
    props, _ := client.GetProperties(context.Background(), nil)
    log.Printf("Blob content type: %s", *props.ContentType)
}
```

## 5. Deployment

### 5.1 Compilation

The user's code must be compiled for the target platform:

```bash
# For Linux (Azure Functions default)
GOOS=linux GOARCH=amd64 go build -o app .

# For Windows
GOOS=windows GOARCH=amd64 go build -o app.exe .

# For local development (current OS)
go build -o app .    # or app.exe on Windows
```

### 5.2 Deployment Artifacts

#### Dedicated Mode (Local Development / Dedicated Plan)

The deployment artifact contains:

1. `app` or `app.exe` (the compiled user binary)
2. `worker.config.json`
3. `host.json`

#### Consumption Mode (with Placeholder Support)

The deployment artifact contains:

1. `app` or `app.exe` (the compiled user binary)
2. `proxy` or `proxy.exe` (the platform interface)
3. `worker.config.json` (points `defaultExecutablePath` to the proxy)
4. `host.json`

### 5.3 `worker.config.json`

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

## 6. Local Development with Core Tools

### 6.1 `func init --worker-runtime golang`

Initializes a new Go Function App project, generating:

- `host.json` with extension bundle configuration
- `local.settings.json` with `FUNCTIONS_WORKER_RUNTIME` set to `"golang"`
- `main.go` with a sample HTTP trigger function
- `go.mod` via `go mod init`

### 6.2 `func start`

Core tools:
1. Detects `FUNCTIONS_WORKER_RUNTIME = "golang"` from `local.settings.json`.
2. Runs `go build -o app .` (or `app.exe` on Windows) to compile the project.
3. Starts the Azure Functions Host, which reads `worker.config.json` and launches `app`.
4. The host and worker communicate over gRPC.
5. HTTP trigger endpoints are displayed (e.g., `http://localhost:7071/api/hello`).

### 6.3 Docker Support

`func init --worker-runtime golang --docker` generates a `Dockerfile`:

```dockerfile
FROM mcr.microsoft.com/azure-functions/dotnet:4

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

COPY . /home/site/wwwroot
```

The user pre-compiles their binary for Linux before building the Docker image.
