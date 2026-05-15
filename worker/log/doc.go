// Package log implements the worker's logging pipeline: the wire
// translation between Go [log/slog] records and the host-side
// FunctionRpc [pb.RpcLog] message, plus the bootstrap stderr handler
// used before the gRPC stream is open.
//
// The package is internal to the worker in spirit (it exists to
// service the dispatcher and the otelfunc bridge), but lives in a
// regular path rather than under internal/ so middleware/otelfunc can
// import [RegisterObserver] without the worker package depending on
// any OTel module.
//
// # Record lifecycle
//
// Every user log record traverses three stages:
//
//  1. Construction. User code calls [slog.InfoContext] (or any other
//     slog method). The SDK's slog handler chain adds invocation_id,
//     function_name, and trigger_type from the [sdk.InvocationContext]
//     attached to the context.
//
//  2. gRPC emission. The chain bottoms out in a [Writer]-backed slog
//     handler ([NewUser] for user-category records, [NewSystem] for
//     worker-internal events). [Writer.Write] builds a [pb.RpcLog]
//     from the record and pushes it onto the dispatcher's outbound
//     gRPC stream. Host-supplied per-category log filters from
//     WorkerInitRequest.LogCategories are applied here.
//
//  3. Observer fan-out. After the RpcLog has been enqueued on the
//     outbound stream, [NewUser]'s handler clones the record and fans
//     it out to every [Observer] registered via [RegisterObserver].
//     The motivating consumer is the otelfunc package, which registers
//     an observer that bridges every user slog record into the
//     configured OpenTelemetry LoggerProvider so logs carry the same
//     trace.id and span.id as the worker invocation span.
//
// The bootstrap stage runs before stage 2 is wired up: during
// argument parsing and gRPC dial, slog records fall through to
// [NewBootstrap], which writes them to stderr with the
// LanguageWorkerConsoleLog prefix the host's stderr capture
// recognizes. Once the stream opens, worker.Start swaps the SDK
// default to a [Writer]-backed handler and bootstrap records stop.
//
// # Order-sensitive slog semantics
//
// The user and system slog handlers preserve the contract that
// attributes bound BEFORE a [slog.Logger.WithGroup] remain at the
// top level while attributes bound AFTER nest inside that group. The
// internal logComposer type tracks the bind-time group stack for each
// attribute, so chains like
//
//	slog.Default().
//	    With("tenant_id", t).
//	    WithGroup("http").
//	    With("method", m, "path", p)
//
// emit "tenant_id" at the top level and "method"/"path" inside the
// "http" group, matching slog.JSONHandler and slog.TextHandler.
//
// # Public surface
//
// Most callers only need:
//
//	- [NewBootstrap] - stderr handler used before the gRPC stream is open.
//	- [NewWriter]    - the gRPC RpcLog sender.
//	- [NewUser]      - user-category slog.Handler that wraps a Writer.
//	- [NewSystem]    - System-category slog.Handler for worker-internal logs.
//	- [RegisterObserver] / [Observer] - the otelfunc integration seam.
package log
