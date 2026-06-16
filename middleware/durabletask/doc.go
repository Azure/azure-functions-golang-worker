// Package durabletask provides Durable Functions support for the
// azure-functions-golang-worker SDK as a self-contained middleware, in the
// same spirit as middleware/otelfunc: the user enables the whole feature
// with a single App.Use call and all durable-specific logic — including the
// heavy durabletask-go dependency — lives in this package.
//
//	import (
//	    "github.com/azure/azure-functions-golang-worker/sdk"
//	    "github.com/azure/azure-functions-golang-worker/middleware/durabletask"
//	    "github.com/azure/azure-functions-golang-worker/worker"
//	    "github.com/microsoft/durabletask-go/task"
//	)
//
//	func main() {
//	    app := sdk.FunctionApp()
//	    app.Use(durabletask.Middleware(
//	        durabletask.WithOrchestrator("HelloCities", HelloCities),
//	        durabletask.WithActivity("SayHello", SayHello),
//	    ))
//	    app.HTTP("start", StartHelloCities, sdk.WithMethods("post"))
//	    worker.Start(app)
//	}
//
// # Execution model
//
// Durable Functions for an out-of-process worker follow a replay model. The
// Functions host's WebJobs DurableTask extension owns all durable state
// (history, queues, dispatch); the worker is a stateless replay engine. This
// mirrors the .NET isolated worker's GrpcOrchestrationRunner.LoadAndRun:
//
//  1. The host invokes an orchestrator like any other function. The trigger
//     input is a base64-encoded protobuf OrchestratorRequest carrying the
//     orchestration history (past + new events).
//  2. The worker replays the orchestrator against that history.
//  3. The orchestrator produces a list of actions (call activity, create
//     timer, complete, …), serialized as a base64 OrchestratorResponse and
//     returned as the function's return value.
//  4. The host applies the actions, persists state, and schedules the next
//     turn.
//
// Activities are ordinary functions: input in, result out. The host's
// DurableTask extension wraps/unwraps the activity protocol, so an activity
// registered here runs through the normal worker pipeline (its return value
// is encoded into the InvocationResponse) without any replay machinery.
//
// # How it maps onto the worker
//
// The package implements [sdk.Middleware] plus two optional contracts:
//
//   - [sdk.Middleware] (Wrap): intercepts orchestration invocations, reads
//     the inbound history via mc.InputBytes, replays via durabletask-go, and
//     records the response via mc.SetReturnValue — short-circuiting the chain
//     so the registered orchestrator placeholder never runs. Every other
//     trigger (activities, HTTP starters, timers) passes through to next.
//   - [sdk.FunctionProvider]: contributes the orchestrator and activity
//     functions to the App so the host receives metadata for them. This is
//     what lets a single App.Use wire the whole feature.
//   - [sdk.ShutdownProvider]: closes the management [Client]'s gRPC
//     connection at worker shutdown when the middleware created it.
//
// # Management client
//
// Starter functions schedule and manage instances through a [Client] obtained
// from the invocation context via [ClientFromContext]. A starter declares it
// needs the client by adding [ClientInput] to its registration, which appends
// a durableClient input binding so the Functions host delivers the durable
// gRPC endpoint with each invocation; the middleware connects to that endpoint
// (once per endpoint, reused across invocations) and attaches the client to
// the context:
//
//	app.HTTP("start", StartHelloCities,
//	    sdk.WithMethods("post"), durabletask.ClientInput())
//
// When no binding endpoint is delivered, the middleware falls back to a client
// dialed from the [EnvGrpcEndpoint] environment variable (if set) or one
// supplied via [WithClient] — both mainly useful for tests and standalone
// scenarios.
//
// # Distributed tracing
//
// No tracing code lives here. Because orchestrator and activity executions
// arrive as ordinary trigger invocations, they flow through the App's normal
// middleware chain — so registering middleware/otelfunc alongside this package
// traces them automatically, with the host-supplied W3C trace context on each
// invocation. otelfunc emits an execution span per orchestrator turn and per
// activity, and HTTP starter functions are traced the same way. Linking a
// starter's span to the orchestration it schedules additionally requires the
// management client to propagate the trace context on the start call, which
// depends on an upstream durabletask-go change and is tracked separately.
//
// # Engine dependency
//
// Replay is delegated to durabletask-go's in-process task executor
// (task.NewTaskExecutor(...).ExecuteOrchestrator). Because the upstream
// OrchestratorRequest / OrchestratorResponse protobuf types currently live
// in an internal package, this package parses the request envelope with
// protowire (see runner.go) and marshals the engine's response value
// directly. If/when durabletask-go exposes a public bytes-in/bytes-out
// runner (e.g. OrchestrationRunner.LoadAndRun), runner.go can be reduced to
// a one-line delegation with no change to this package's public API.
package durabletask
