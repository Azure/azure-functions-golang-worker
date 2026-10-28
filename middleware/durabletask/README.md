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
	durable.Entity("counter", Counter)
	app.Use(durable)

	app.HTTP("start", StartHelloCities,
		sdk.WithMethods("post"), durabletask.ClientInput())

	worker.Start(app)
}
```

API reference: <https://pkg.go.dev/github.com/azure/azure-functions-golang-worker/middleware/durabletask>.
A complete application is in [`samples/durableFunctions`](../../samples/durableFunctions).

## Blob payload offloading

Offloading is opt-in. Construct durabletask-go's Azure Blob store once, then
give its options to the middleware before registering functions:

```go
import (
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/payload"
)

store, err := payload.NewAzureBlobStore(payload.AzureBlobStoreOptions{
	ConnectionString: os.Getenv("DURABLE_PAYLOAD_STORAGE"),
})
if err != nil {
	log.Fatal(err)
}
options := &api.LargePayloadOptions{Store: store, Resolver: store}
durable := durabletask.Middleware(durabletask.WithLargePayloads(options))
durable.Orchestrator("HelloCities", HelloCities)
durable.Activity("SayHello", SayHello)
app.Use(durable)
```

Alternatively, set `AccountURL` and `Credential` on `AzureBlobStoreOptions`
for token authentication. Exactly one authentication mode is required. The
store owns URL and token validation, compression, and integrity checks. Its
defaults are a `durabletask-payloads` container, gzip compression, an inclusive
256 KiB threshold, and a 10 MiB uncompressed payload limit. Thresholds measure
the serialized UTF-8 JSON bytes, not the size of the Go value or compressed
blob. `api.LargePayloadOptions` can override the threshold and limit subject
to the store's validation. Invalid options panic during middleware/client
construction rather than failing only when a large payload first appears.

The same configuration covers orchestration history and actions, activity
inputs and results, entity state, operation inputs and results, entity actions,
and management requests and responses supported by durabletask-go. Application
handlers see their original JSON values, not storage references. The upstream
store writes native `blob:v2:` tokens and reads `blob:v1:` as well.

Functions needs a distinct transport representation. Its activity binding parses
inputs as JSON before dispatch, and the Azure Storage backend interprets raw
absolute URIs as its own large-message references. This middleware therefore
transports upstream references inside a versioned JSON object:

```json
{"$durabletaskPayload":{"version":1,"reference":"blob:v2:https://account.blob.core.windows.net/durabletask-payloads/example"}}
```

`$durabletaskPayload` is reserved at the top level of a payload object.
Application outputs and management inputs, including raw-input options, using that key are rejected rather
than accidentally hydrated. Ordinary strings containing tokens or envelope JSON,
and objects with the key nested beneath an application property, remain unchanged.
Malformed recognized envelopes and unsupported versions fail explicitly.
Reserved references fail when a compatible store is unavailable.
Custom token stores use the same envelope for tokens recognized by their
`IsLargePayloadToken` method, including opaque absolute URLs. Configure the same
token store on clients and execution middleware. Quoted application strings are
not token references.

This envelope is a Go Functions middleware protocol, not the Functions
extension's built-in `payloadStorageEnabled` feature. All consumers of these
histories must understand it. Raw DTS token consumers and other SDKs do not
automatically understand the envelope. Sharing the upstream blob format alone
does not establish cross-SDK orchestration interoperability.

Clients supplied through `WithClient`, created from `DURABLE_TASK_GRPC_ENDPOINT`,
or cached from a `ClientInput()` binding use the middleware's configuration.
Without `WithLargePayloads`, middleware adopts an explicitly supplied client's
configuration. An explicit middleware option takes precedence without modifying
the caller's client. For an independently used client, use the same options:

```go
client, err := durabletask.Dial(endpoint,
	durabletask.WithClientLargePayloads(options))
// Or NewClient(conn, durabletask.WithClientLargePayloads(options)).
```

The caller still closes clients supplied through `WithClient`. Middleware closes
only connections it creates. With no payload configuration, existing inline
payload behavior is unchanged. `AzureWebJobsStorage` never enables this feature
implicitly.

All participating workers and management clients need access to the same blob
store. The host's built-in HTTP status endpoint is not configured by this
middleware and may expose reference envelopes. Use the configured Go client when hydrated
management values are required. Blob lifecycle and retention are application
responsibilities. Failed turns or retried writes can leave unreferenced blobs,
so do not delete blobs that an active instance's history may still reference.
Do not perform blob I/O in orchestrator user code. The adapter hydrates before
replay and externalizes actions after execution, outside deterministic replay.

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
	durabletask.WithEntity("counter", Counter),
))
```

## Entities

Register a Functions entity with an operation handler. The host delivers each
serialized operation batch to the upstream durabletask-go executor, which
applies operations in order and returns state changes, results, and emitted
actions to the host:

```go
func Counter(ctx *durabletask.EntityContext) (any, error) {
	count := 0
	if ctx.HasState() {
		if err := ctx.GetState(&count); err != nil {
			return nil, err
		}
	}
	switch ctx.OperationName {
	case "add":
		var increment int
		if err := ctx.GetInput(&increment); err != nil {
			return nil, err
		}
		count += increment
		if err := ctx.SetState(count); err != nil {
			return nil, err
		}
		return count, nil
	case "get":
		return count, nil
	default:
		return nil, fmt.Errorf("unknown operation %q", ctx.OperationName)
	}
}
```

`EntityContext` exposes the entity ID, operation name, request ID, batch
position and size, state and input helpers, outgoing immediate or scheduled
signals, and orchestration starts. `EntityFactory` can create a handler and
cleanup callback per batch. There is intentionally no reflected entity-method
API; the Functions host invokes named operations through the explicit handler.

Orchestrations can use durabletask-go's entity calls, signals, and locks. The
adapter translates the Functions host's legacy event history during replay.
Configure the host's `entityMessageReorderWindowInMinutes` to `0` for
orchestrations that use entities. durabletask-go does not implement a positive
reorder window, so entity history or emitted entity actions with a nonzero
window fail explicitly rather than silently changing the ordering guarantee.
The host's default rollback behavior is supported; setting
`rollbackEntityOperationsOnExceptions` to `false` is rejected.

The Functions entity batch does not identify whether an operation arrived as a
call or a signal. Accordingly, `EntityContext` does not expose
durabletask-go's `IsSignal`; entity handlers must not rely on distinguishing
those request types. The host still routes calls and signals, and uses results
only for calls. In orchestration history, the adapter uses the upstream
operation-sent event only as an action acknowledgement; it does not infer
call-versus-signal metadata for entity handlers.

The inherited management client exposes `SignalEntity`, `GetEntity`, and
`QueryEntities` unchanged. These are exercised against the Functions host's
Azure Storage provider in the integration suite. Other providers must support
the corresponding Durable Task gRPC methods.

## Sub-orchestrations

Give a sub-orchestration an explicit instance ID when you need a predictable
one:

```go
ctx.CallSubOrchestrator("ProcessLine",
	task.WithSubOrchestrationInstanceID(string(ctx.ID)+":line-1"),
	task.WithSubOrchestratorInput(line))
```

Without one the worker generates `<parent>:<action id>`, the same format
durabletask-go used for its former standalone backend.

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

Orchestrator and activity executions arrive as ordinary trigger invocations.
Registering `middleware/otelfunc` wraps their execution in spans, using inbound
trace context when available. The replay runner also annotates an active span
with per-turn instance and event information.

Durable registers a callable replay adapter instead of executing replay in its
middleware. Both `app.Use(durable)` before `app.Use(otelfunc.Middleware())` and
the reverse order allow tracing to wrap replay. Ordinary middleware still runs
in registration order and can deliberately stop downstream execution.

This changes behavior for apps that previously registered middleware after
Durable: those middleware now run for orchestrator invocations too. Scope
trigger-specific policies explicitly, and keep non-deterministic application
work in activities. A middleware that needs `ClientFromContext` before calling
`next` must follow Durable; Durable does not inject a client into orchestrations.

The worker binds arguments before middleware runs, but the replay adapter reads
`MiddlewareContext.InputString()` at execution time. This preserves the late
input read from the previous middleware implementation: replacements made with
`SetInputString` before `next` reach replay. A present carrier's `InputString()`
result is authoritative, even when it is empty or malformed; the runner validates
that result rather than falling back to the bound argument. Direct adapter calls
without a carrier use their explicit argument.

Middleware must preserve the invocation carrier when passing a context to `next`.
Derive it from the supplied `ctx` (including `context.WithoutCancel` when needed),
or explicitly attach `mc` with `sdk.ContextWithMiddleware`. A fresh
`context.Background()` drops the carrier, while `sdk.NewContext` creates a new,
empty carrier. Neither preserves late input changes by itself.

This is Durable-specific compatibility, not a general rebinding feature for
ordinary functions. It preserves the existing string/byte accessor semantics;
it does not change cross-representation cache invalidation. For example, clearing
only the string after reading `InputBytes()` can still expose cached bytes through
`InputString()`, as before. Replay input is a
protocol envelope containing history, not just the application's start input.
Middleware can also inspect the replay return value after `next`, or deliberately
replace it using `SetReturnValue`.

### Logging

Durable uses the application's default `slog` logger, which the SDK normally
routes through Functions user logging. A custom `slog.SetDefault` is respected.
The middleware does not replace the worker's separate System logger.

Inside an orchestrator, use `ctx.Logger()` rather than global `slog` calls.
Upstream suppresses this logger during replay and attaches Durable identity
attributes. The middleware binds log delivery to the current replay invocation,
so invocation metadata and the active span reach the existing worker logging
pipeline, including the `otelfunc` log bridge when a log provider is configured.
This does not expose host context values to deterministic orchestrator code.
Activities can use ordinary `slog.InfoContext(ctx, ...)` calls.

The management client's context-free diagnostic logger also uses `slog`, with
structured severity instead of text prefixes. Clients are shared across
invocations, so those diagnostics are not bound to any one invocation or span.

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

## Known limitations

- This module uses **durabletask-go v1.0.0-beta3** and requires Go 1.25 or later.
  Upstream's protobuf messages are now internal. Replay still uses its task
  executor, with a wire-level request envelope and inferred history event types.
- The Functions host entity protocol does not preserve upstream's
  call-versus-signal `IsSignal` metadata. The entity API deliberately omits it.
- Positive entity reorder windows and disabling entity-operation rollback are
  rejected because the pinned upstream executor cannot honor those settings.
- **Extended sessions** are rejected by the host for non-.NET workers, so the
  worker does not implement session caching. It fails loudly rather than
  replaying a withheld history.
- **Rewind** appears in the management URLs for parity with the other workers,
  but only the Azure Storage backend implements it.

Unit tests use small descriptor-based histories and the upstream executor for
individual replay turns. Fixed gRPC replies check management request encoding,
binding endpoint selection, client reuse, shutdown, and not-found translation.
These stubs never execute orchestrators or manufacture scheduling history.

Lifecycle semantics belong to the real host tests in `tests/integration`.
They exercise the Durable extension with isolated Azurite storage, including
activities, fan-out, external events, custom status, durable timeouts, the Go
management wait APIs, and restart/resume. Neither unit nor local host tests
establish compatibility with the Durable Task Scheduler service.

With beta3, an orchestrator panic is recorded as a failed orchestration with
failure details by the upstream executor. Like an orchestrator returning an
error, it produces a successfully processed replay response. It does not escape
to middleware recovery or mark the replay invocation span as an error. Request
decoding and transport failures still surface as invocation errors.
