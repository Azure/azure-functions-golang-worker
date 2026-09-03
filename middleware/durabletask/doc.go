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
//
//	    durable := durabletask.Middleware()
//	    durable.Orchestrator("HelloCities", HelloCities)
//	    durable.Activity("SayHello", SayHello)
//	    app.Use(durable)
//
//	    app.HTTP("start", StartHelloCities,
//	        sdk.WithMethods("post"), durabletask.ClientInput())
//	    worker.Start(app)
//	}
//
// Register orchestrators and activities before app.Use, which is when the app
// collects them. Registering afterwards panics rather than leaving a function
// that is never indexed. They can also be supplied as construction options,
// which suits an app small enough to fit in one expression:
//
//	app.Use(durabletask.Middleware(
//	    durabletask.WithOrchestrator("HelloCities", HelloCities),
//	    durabletask.WithActivity("SayHello", SayHello),
//	))
//
// # Execution model
//
// Durable Functions for an out-of-process worker follow a replay model. The
// Functions host's WebJobs DurableTask extension owns all durable state
// (history, queues, dispatch); the worker is a stateless replay engine. This
// mirrors the .NET isolated worker's GrpcOrchestrationRunner.LoadAndRun: the
// host invokes an orchestrator like any other function, passing the history as
// a base64-encoded protobuf OrchestratorRequest; the worker replays the
// orchestrator against it and returns the resulting actions as a base64
// OrchestratorResponse; the host applies them and schedules the next turn.
//
// Activities are ordinary functions: input in, result out. The host wraps and
// unwraps the activity protocol, so an activity registered here runs through
// the normal worker pipeline without any replay machinery.
//
// # How it maps onto the worker
//
// The package implements [sdk.Middleware] plus three optional contracts:
//
//   - [sdk.Middleware] (Wrap): intercepts orchestration invocations, reads
//     the inbound history via mc.InputString, replays via durabletask-go, and
//     records the response via mc.SetReturnValue — short-circuiting the chain
//     so the registered orchestrator placeholder never runs. Every other
//     trigger (activities, HTTP starters, timers) passes through to next.
//   - [sdk.FunctionProvider]: contributes the orchestrator and activity
//     functions to the App so the host receives metadata for them. This is
//     what lets a single App.Use wire the whole feature.
//   - [sdk.RegistrationSealer]: learns when App.Use has taken those
//     registrations, so a later one fails loudly instead of landing in a
//     registry the app will never read again.
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
// [Client] embeds durabletask-go's client, so every management operation it
// offers is available directly. This package adds only what that client cannot
// know about: connection ownership, and the Durable Functions HTTP contract
// ([Client.WriteCheckStatusResponse], [WriteStatusResponse], [RuntimeStatus]).
//
// When no binding endpoint is delivered, the middleware falls back to a client
// dialed from the [EnvGrpcEndpoint] environment variable (if set) or one
// supplied via [WithClient] — both mainly useful for tests and standalone
// scenarios.
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
//
// # Further reading
//
// The package README covers the operational side: the extension bundle
// requirement, Azure Storage versus Durable Task Scheduler, running locally
// with Core Tools and Azurite, and the current gaps.
package durabletask
