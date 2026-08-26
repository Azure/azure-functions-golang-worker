# Durable Functions sample

Demonstrates Durable Functions in the Go worker using the
`middleware/durabletask` package. The whole feature is enabled with a single
`app.Use(durabletask.Middleware(...))` call.

The app registers **two orchestrations** to show how multiple workflows live
side by side in one app, plus the three ways orchestration endpoints are
exposed (start, status/progress, and the human-in-the-loop response).

## Functions

| Function | Trigger | Role |
|---|---|---|
| `HelloCities` | `orchestrationTrigger` | Orchestrator: calls `SayHello` for each city in sequence |
| `ProcessExpense` | `orchestrationTrigger` | Orchestrator: fan-out/fan-in validation + HITL approval with a durable timeout |
| `SayHello` | `activityTrigger` | Activity for `HelloCities` |
| `ValidateReceipt` / `CheckPolicy` / `CheckBudget` | `activityTrigger` | Parallel checks for `ProcessExpense` |
| `RecordDecision` | `activityTrigger` | Records the final expense outcome |
| `StartHelloCities` | `httpTrigger` POST `/api/hello` | Starts `HelloCities`, returns the instance ID |
| `SubmitExpense` | `httpTrigger` POST `/api/expenses` | Starts `ProcessExpense`, returns the check-status payload |
| `GetExpenseStatus` | `httpTrigger` GET `/api/expenses/{id}` | Returns runtime + custom status (progress) |
| `ApproveExpense` | `httpTrigger` POST `/api/expenses/{id}/approve` | HITL: raises the `ApprovalDecision` event |

## How it works

- **Orchestrators** use the durabletask-go programming model
  (`task.OrchestrationContext`: `CallActivity`, `CreateTimer`,
  `WaitForSingleEvent`, `SetCustomStatus`, …). The host sends the
  orchestration history to the worker; the durable middleware replays the
  orchestrator and returns the resulting actions. Orchestrator code must be
  deterministic.
- **Activities** are ordinary functions (input in, result out) and run
  through the normal worker pipeline, so non-deterministic code is fine.
- **Endpoints** are ordinary HTTP functions that reach the durable client via
  `durabletask.ClientFromContext(r.Context())`. Each starter adds
  `durabletask.ClientInput()` to its registration so the host delivers the
  durable gRPC endpoint with the invocation; the middleware connects to it and
  attaches the client to the request context.

### Defining more than one orchestration

Register each orchestrator (and its activities) on the same middleware — the
host dispatches each by name:

```go
durable := durabletask.Middleware()

durable.Orchestrator("HelloCities", HelloCities)
durable.Orchestrator("ProcessExpense", ProcessExpense)
durable.Activity("SayHello", SayHello)
durable.Activity("ValidateReceipt", ValidateReceipt)
// ...

app.Use(durable)
```

Register everything before `app.Use`, which is when the app collects the
functions. Registering afterwards panics rather than leaving an orchestration
that is never indexed and so never runs.

The same registrations can be supplied as construction options instead, which
suits a small app that fits in one expression:

```go
app.Use(durabletask.Middleware(
    durabletask.WithOrchestrator("HelloCities", HelloCities),
    durabletask.WithActivity("SayHello", SayHello),
))
```

### How the endpoints are exposed

The host's DurableTask extension also exposes a built-in management REST API
under `/runtime/webhooks/durabletask/…` (status, raiseEvent, terminate,
purge). You can either return those URLs to clients (the check-status payload)
or wrap them in your own functions, as this sample does:

- **Start** — a normal HTTP function calls `client.ScheduleNewOrchestration`.
  `SubmitExpense` then calls `client.WriteCheckStatusResponse` (HTTP 202 + the
  management URLs), the canonical Durable Functions starter response.
  `StartHelloCities` shows the minimal alternative, returning just the new
  instance ID.
- **Status / progress** — `GetExpenseStatus` calls `client.GetStatus(id)`.
  The orchestrator reports progress with `ctx.SetCustomStatus(...)`, which
  surfaces as `customStatus` in the response.
- **HITL response** — `ApproveExpense` calls
  `client.RaiseEvent(id, "ApprovalDecision", payload)`, delivering the event
  the orchestrator is blocked on in `WaitForSingleEvent`.

## Run locally

Requires the Azure Functions Core Tools, the Go worker installed, and an
Azure Storage emulator (Azurite) for the DurableTask state store.

This sample is its own Go module because it pulls in the durable middleware,
so create the module first. Point the module at the local checkout so it builds
against this repository rather than a published release:

```bash
cd samples/durableFunctions
go mod init durable-functions-sample
go mod edit \
  -replace github.com/azure/azure-functions-golang-worker=../.. \
  -replace github.com/azure/azure-functions-golang-worker/middleware/durabletask=../../middleware/durabletask \
  -replace github.com/azure/azure-functions-golang-worker/middleware/otelfunc=../../middleware/otelfunc \
  -replace github.com/azure/azure-functions-golang-worker/otelcollector=../../otelcollector
go mod tidy
```

> If you keep a `go.work` file at the repository root, either add this sample to
> it or run Core Tools with `GOWORK=off`. A workspace that does not list the
> sample makes the build resolve the root module instead and fail.

Then start the host:

```bash
# from the repo root, start Azurite (or use the emulators compose file)
docker compose -f tests/emulators/docker-compose.yml up -d azurite

cd samples/durableFunctions
func start
```

> Durable Functions needs a DurableTask extension that recognizes the `native`
> worker runtime and selects the gRPC durable protocol. That landed in
> DurableTask 3.15.0, which the experimental bundle above carries. Without it
> the starter endpoints return HTTP 500.

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
> extension bundle configured in `host.json`. The Go worker is a stateless
> replay engine; it does not own durable state.

## Tests

The orchestration patterns are covered by emulator-backed tests in
[`middleware/durabletask`](../../middleware/durabletask):

- replay equivalence (decode → replay → encode matches the engine directly),
- the middleware short-circuit and activity pass-through paths,
- a full end-to-end run of the sequential orchestrator, and
- a full end-to-end run of fan-out/fan-in **plus the external-event (HITL)
  approval**, driven to completion on the in-memory durabletask backend.
