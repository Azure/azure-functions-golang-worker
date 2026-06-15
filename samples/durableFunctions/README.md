# Durable Functions sample

Demonstrates Durable Functions in the Go worker using the
`middleware/durabletask` package, on the **model 2** (gRPC work-item stream)
execution model: the Functions host's DurableTask extension owns durable state
and dispatches work over a local gRPC sidecar; the worker runs a work-item
listener that executes orchestrators and activities.

The app registers **two orchestrations** to show how multiple workflows live
side by side in one app, plus the three ways orchestration endpoints are
exposed (start, status/progress, and the human-in-the-loop response).

## Functions

| Function | Kind | Role |
|---|---|---|
| `HelloCities` | orchestrator | Calls `SayHello` for each city in sequence |
| `ProcessExpense` | orchestrator | Fan-out/fan-in validation + HITL approval with a durable timeout |
| `SayHello` | activity | Activity for `HelloCities` |
| `ValidateReceipt` / `CheckPolicy` / `CheckBudget` | activity | Parallel checks for `ProcessExpense` |
| `RecordDecision` | activity | Records the final expense outcome |
| `StartHelloCities` | `httpTrigger` POST `/api/hello` | Starts `HelloCities`, returns the instance ID |
| `SubmitExpense` | `httpTrigger` POST `/api/expenses` | Starts `ProcessExpense`, returns the status URL |
| `GetExpenseStatus` | `httpTrigger` GET `/api/expenses/{id}` | Returns runtime + custom status (progress) |
| `ApproveExpense` | `httpTrigger` POST `/api/expenses/{id}/approve` | HITL: raises the `ApprovalDecision` event |

## How it works

- **Orchestrators** use the durabletask-go programming model
  (`func(*task.OrchestrationContext) (any, error)`: `CallActivity`,
  `CreateTimer`, `WaitForSingleEvent`, `SetCustomStatus`, …). The work-item
  listener replays them as the host dispatches turns. Orchestrator code must be
  deterministic.
- **Activities** use durabletask-go's native signature
  (`func(task.ActivityContext) (any, error)`); they read input with
  `ctx.GetInput(&v)` and run once (no replay), so non-deterministic code is
  fine.
- **Endpoints** are ordinary HTTP functions that reach the durable client via
  `durabletask.ClientFromContext(r.Context())`.

### Wiring (one registration)

```go
app := sdk.FunctionApp()

// One App.Use wires the whole feature: it injects the durable client into
// starter invocations and contributes the work-item listener (which executes
// orchestrators + activities); the worker starts and stops it automatically.
app.Use(durabletask.Middleware(
    durabletask.WithOrchestrator("HelloCities", HelloCities),
    durabletask.WithOrchestrator("ProcessExpense", ProcessExpense),
    durabletask.WithActivity("SayHello", SayHello),
    durabletask.WithActivity("ValidateReceipt", ValidateReceipt),
    // ...
))
app.HTTP("StartHelloCities", StartHelloCities, sdk.WithMethods("post"), sdk.WithRoute("hello"))
// ... other HTTP endpoints ...

worker.Start(app)
```

The host durable gRPC endpoint comes from `durabletask.WithEndpoint(...)` or
the `DURABLE_TASK_GRPC_ENDPOINT` environment variable.

### How the endpoints are exposed

The management endpoints are ordinary HTTP functions that use the durable
[`Client`](../../middleware/durabletask) (a thin wrapper over durabletask-go's
`TaskHubGrpcClient`) injected into their request context:

- **Start** — `client.ScheduleNewOrchestration(name, input)`. `SubmitExpense`
  returns a 202 with a `statusQueryGetUri` pointing at its own status route.
- **Status / progress** — `GetExpenseStatus` calls `client.GetStatus(id)`.
  The orchestrator reports progress with `ctx.SetCustomStatus(...)`, which
  surfaces as `customStatus` in the response.
- **HITL response** — `ApproveExpense` calls
  `client.RaiseEvent(id, "ApprovalDecision", payload)`, delivering the event
  the orchestrator is blocked on in `WaitForSingleEvent`.

## Run locally

Requires the Azure Functions Core Tools, the Go worker installed, and an
Azure Storage emulator (Azurite) for the DurableTask state store.

```bash
# from the repo root, start Azurite (or use the emulators compose file)
docker compose -f tests/emulators/docker-compose.yml up -d azurite

cd samples/durableFunctions
func start
```

### Simple orchestration

```bash
curl -X POST http://localhost:7071/api/hello
# => { "id": "<instanceId>" }
```

### Expense workflow with human approval

```bash
# 1. Submit an expense above the auto-approve limit ($100). The 202 response
#    body contains statusQueryGetUri / sendEventPostUri / terminatePostUri.
curl -i -X POST http://localhost:7071/api/expenses \
  -H 'Content-Type: application/json' \
  -d '{"id":"E-1001","submitter":"sam","category":"travel","amount":750,"receiptUrl":"https://x/r.pdf"}'

# 2. Poll progress — runtimeStatus + customStatus ("awaiting manager approval").
curl http://localhost:7071/api/expenses/<instanceId>

# 3. Approve (the HITL response) — raises the ApprovalDecision event.
curl -X POST http://localhost:7071/api/expenses/<instanceId>/approve \
  -H 'Content-Type: application/json' \
  -d '{"approved":true,"by":"manager-jane"}'

# 4. Poll again — runtimeStatus Completed, output { status: "approved", ... }.
curl http://localhost:7071/api/expenses/<instanceId>
```

> The DurableTask extension (state store, dispatch) is provided by the host's
> extension bundle configured in `host.json`. The Go worker connects to the
> extension's durable gRPC sidecar, runs the work-item listener, and does not
> own durable state.
>
> **Note:** a full `func`-host run additionally requires host-side support for
> the Go runtime (selecting the gRPC durable protocol and delivering the
> sidecar endpoint to the worker). Until then, the patterns are validated by
> the gRPC-sidecar tests below.

## Tests

The orchestration patterns are covered by gRPC-sidecar tests in
[`middleware/durabletask`](../../middleware/durabletask):

- the work-item listener executing an orchestrator + activities end to end
  (the host dispatches over gRPC, the worker's listener executes),
- fan-out/fan-in **plus the external-event (HITL) approval** driven to
  completion through the listener,
- the management client (schedule, status/custom-status, raise event,
  wait-for-completion, not-found), and
- the middleware injecting the durable client into invocation context.
