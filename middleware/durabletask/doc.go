// Package durabletask provides Durable Functions support for the
// azure-functions-golang-worker SDK, built on durabletask-go.
//
// # Execution model (model 2: gRPC work-item stream)
//
// The Functions host's DurableTask extension owns all durable state and
// exposes a local gRPC sidecar (the TaskHubSidecarService). The worker
// connects to that sidecar and plays two roles over the one connection:
//
//   - Work-item listener — durabletask-go's StartWorkItemListener pulls
//     orchestrator and activity work items from the host and executes them in
//     the worker process. Orchestrators are replayed across turns; activities
//     run once. This is started by [Durable] as a [sdk.LifecycleHook].
//   - Management client — starter functions schedule and manage instances
//     (start, raise event, query status, terminate). The [Client] wraps
//     durabletask-go's TaskHubGrpcClient and is injected into invocation
//     context by [Durable.Middleware].
//
// This mirrors how the .NET isolated and Java workers do durable: the host
// extension is the backend; the worker is a durabletask client + work-item
// listener. Execution does NOT flow through the normal FunctionRpc trigger
// pipeline — orchestrators/activities run in durabletask-go's listener loop.
//
// # Programming model
//
// Orchestrators and activities use durabletask-go's native types:
//
//	func HelloCities(ctx *task.OrchestrationContext) (any, error) {
//	    var out []string
//	    for _, city := range []string{"Tokyo", "Seattle", "London"} {
//	        var r string
//	        if err := ctx.CallActivity("SayHello", task.WithActivityInput(city)).Await(&r); err != nil {
//	            return nil, err
//	        }
//	        out = append(out, r)
//	    }
//	    return out, nil
//	}
//
//	func SayHello(ctx task.ActivityContext) (any, error) {
//	    var city string
//	    if err := ctx.GetInput(&city); err != nil {
//	        return nil, err
//	    }
//	    return "Hello, " + city + "!", nil
//	}
//
// Orchestrator code is replayed, so it must be deterministic; do all I/O and
// non-determinism inside activities.
//
// # Wiring
//
// A single App.Use call wires the whole feature: it injects the durable client
// into starter invocations and contributes the work-item listener, which the
// worker starts and stops automatically (via [sdk.LifecycleProvider]).
//
//	app := sdk.FunctionApp()
//	app.Use(durabletask.Middleware(
//	    durabletask.WithOrchestrator("HelloCities", HelloCities),
//	    durabletask.WithActivity("SayHello", SayHello),
//	))
//	app.HTTP("start", StartHelloCities, sdk.WithMethods("post"))
//
//	worker.Start(app) // listener starts automatically; no extra hook to register
//
// The host durable gRPC endpoint is taken from [WithEndpoint], [WithConnection],
// or the EnvGrpcEndpoint environment variable.
//
// # Distributed tracing (planned)
//
// Full trace correlation across the chain (host → starter → durable client →
// host sidecar → worker execution) is layered on via middleware/otelfunc and
// depends on upstream durabletask-go changes (client-side spans, propagation
// of ParentTraceContext, and worker-side execution spans). Until then, starter
// functions are traced by otelfunc as normal; orchestration/activity execution
// spans arrive once that wiring lands.
//
// # Status
//
// The execution path (registry + listener) and the management client run
// end-to-end against a durabletask gRPC sidecar (see the package tests). A
// full Functions-host run additionally requires host-side Go durable support
// (the host must select the gRPC protocol for the Go runtime and deliver the
// sidecar endpoint to the worker).
package durabletask
