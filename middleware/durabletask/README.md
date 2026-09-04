# durabletask

Durable Functions for the Azure Functions Go worker.

Orchestrators and activities are written with the
[durabletask-go](https://github.com/microsoft/durabletask-go) programming model
and registered through a single middleware. The Functions host owns all durable
state; this package makes the worker a replay engine for it.

```go
func main() {
	app := sdk.FunctionApp()

	durable := durabletask.Middleware()
	durable.Orchestrator("HelloCities", HelloCities)
	durable.Activity("SayHello", SayHello)
	app.Use(durable)

	app.HTTP("start", StartHelloCities,
		sdk.WithMethods("post"), durabletask.ClientInput())

	worker.Start(app)
}
```

API reference: <https://pkg.go.dev/github.com/azure/azure-functions-golang-worker/middleware/durabletask>.
A complete application is in [`samples/durableFunctions`](../../samples/durableFunctions).

## Requirements

Durable Functions needs an extension bundle whose DurableTask extension is
**3.15.0 or later**. That release added recognition of the `native` worker
runtime the Go worker uses; without it the host falls back to a legacy HTTP
protocol and durable bindings do not work.

```jsonc
// host.json
{
  "version": "2.0",
  "extensionBundle": {
    "id": "Microsoft.Azure.Functions.ExtensionBundle.Experimental",
    "version": "[4.7.0, 5.0.0)"
  },
  "extensions": {
    "durableTask": {
      "hubName": "MyTaskHub"
    }
  }
}
```

The experimental bundle is served from the default CDN, so no source override
is required. Check which extension a bundle carries with:

```powershell
(Get-Item "$env:USERPROFILE\.azure-functions-core-tools\Functions\ExtensionBundles\Microsoft.Azure.Functions.ExtensionBundle.Experimental\4.7.0\bin\Microsoft.Azure.WebJobs.Extensions.DurableTask.dll").VersionInfo.FileVersion
```

## Backends

The same application code runs against either backend; only `host.json` changes.

**Azure Storage** is the default and needs no `storageProvider` block.

**Durable Task Scheduler** adds one:

```jsonc
"storageProvider": {
  "type": "azureManaged",
  "connectionStringName": "DURABLE_TASK_SCHEDULER_CONNECTION_STRING"
}
```

with an app setting of the form
`Endpoint=https://<scheduler>.<region>.durabletask.io;Authentication=DefaultAzure;TaskHub=<hub>`.
The identity needs the **Durable Task Data Contributor** role on the scheduler.
DTS also provides a dashboard, whose URL is a property of the **task hub**
resource rather than the scheduler.

## Registration order

`app.Use` collects the middleware's functions at the moment it is called, so
registrations must come first:

```go
durable := durabletask.Middleware()
durable.Orchestrator("HelloCities", HelloCities) // before
app.Use(durable)
durable.Activity("SayHello", SayHello)           // panics
```

Registering afterwards panics with an explanation rather than leaving a
function the host never indexes. Supplying everything as construction options
avoids the ordering question entirely:

```go
app.Use(durabletask.Middleware(
	durabletask.WithOrchestrator("HelloCities", HelloCities),
	durabletask.WithActivity("SayHello", SayHello),
))
```

## Sub-orchestrations

Give a sub-orchestration an explicit instance ID when you need a predictable
one:

```go
ctx.CallSubOrchestrator("ProcessLine",
	task.WithSubOrchestrationInstanceID(string(ctx.ID)+":line-1"),
	task.WithSubOrchestratorInput(line))
```

Without one the worker generates `<parent>:<action id>`, matching what the
durabletask-go backend produces when it runs standalone.

## The management client

Starters reach the client through the invocation context. Declare the
dependency with `ClientInput()` so the host delivers its durable gRPC endpoint:

```go
app.HTTP("start", StartHelloCities,
	sdk.WithMethods("post"), durabletask.ClientInput())
```

`Client` embeds durabletask-go's `TaskHubGrpcClient`, so every operation that
client offers is available directly. This package adds only what the upstream
client cannot know about: connection ownership (`Close`) and the Durable
Functions HTTP contract (`ManagementURLs`, `WriteCheckStatusResponse`,
`WriteStatusResponse`, `RuntimeStatus`).

```go
id, err := client.ScheduleNewOrchestration(ctx, "ProcessExpense", api.WithInput(expense))
_ = client.WriteCheckStatusResponse(w, r, string(id))
```

`StartWorkItemListener` is deliberately unsupported: under Functions the host
dispatches work, and its durable endpoint serves no work items.

## Tracing

There is no tracing code in this package. Orchestrator and activity executions
arrive as ordinary trigger invocations, so registering `middleware/otelfunc`
alongside this one traces them, using the W3C trace context the host supplies.
Register `otelfunc` first so it wraps durable's replay short-circuit.

Linking a starter's span to the orchestration it schedules needs the client to
propagate trace context on the start call, which depends on an upstream
durabletask-go change and is tracked separately.

## Running locally

Core Tools 4.12.0 or later ships the native Go worker.

```bash
azurite --silent --skipApiVersionCheck --location ./.azurite \
  --blobPort 10000 --queuePort 10001 --tablePort 10002

cd samples/durableFunctions
GOWORK=off func start
```

`GOWORK=off` matters: a Go workspace at the repository root makes Core Tools'
build resolve the sample against the root module and fail.

## Known gaps

- **Entities** are not supported; durabletask-go has no entity support yet.
- **Extended sessions** are rejected by the host for non-.NET workers, so the
  worker does not implement session caching. It fails loudly rather than
  replaying a withheld history.
- **Rewind** appears in the management URLs for parity with the other workers,
  but only the Azure Storage backend implements it.
