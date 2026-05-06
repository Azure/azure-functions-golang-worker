// Package sdk provides the core function-app and middleware abstractions for
// the Azure Functions Go worker. This file documents the middleware design
// intent.
//
// # Middleware extensibility
//
// The Middleware shape (next Handler -> Handler) is deliberately minimal,
// matching the patterns established by net/http, gRPC interceptors, and
// aws-lambda-go's otellambda. It supports the full range of cross-cutting
// concerns: distributed tracing, structured logging, authentication, retry
// policies, panic recovery, and request/response validation.
//
// Middleware that needs to *replace* function execution entirely (for
// example, a hypothetical Durable Functions runtime that performs
// orchestration replay rather than direct invocation) is not supported by
// a typed extension point today. Such middleware can either:
//
//   - Short-circuit the chain (skip next()) and reimplement reflective
//     handler dispatch internally, or
//   - Wait for a future framework-side feature mechanism, designed when
//     a concrete need emerges.
//
// We deliberately defer adding such a mechanism (analogous to
// IInvocationFeatures in the .NET isolated worker) until a real consumer
// appears, to avoid committing to an API shape that may not match the
// actual requirements.
package sdk

import "context"

// Handler is the unit a Middleware wraps. It runs a single function invocation
// and returns nil on success or an error that becomes the failure status on
// the InvocationResponse.
//
// The InvocationContext passed in is the same one stashed on ctx via
// NewContext; it is provided as a separate argument purely for ergonomics
// (middleware authors avoid an extra FromContext lookup). Its fields are
// read-only from a middleware's perspective — the dispatcher fills them
// before the chain runs.
//
// Note: Handler is the worker-internal function shape that middleware
// composes around. User-facing handler signatures (HTTPHandler, TimerHandler,
// etc.) remain unchanged; the dispatcher constructs the innermost Handler
// from the user's typed function via reflection.
type Handler func(ctx context.Context, ic *InvocationContext) error

// Middleware decorates a Handler with cross-cutting behavior such as
// distributed tracing, structured logging, exception capture, or short-circuit
// authorization.
//
// Idiomatic Go shape: a Middleware receives the next Handler in the chain and
// returns a new Handler that may run code before/after calling next, may
// transform ctx (for example, by attaching an OpenTelemetry span), or may
// short-circuit by returning without calling next.
//
// Third-party modules can author and ship Middleware implementations against
// this shape without any coordination with the worker; the bundled
// middleware/otelfunc package is itself a regular consumer of this API.
type Middleware func(next Handler) Handler

// Use registers a Middleware. Middleware run in registration order: the
// first registered is the outermost — i.e. it observes the invocation first
// and last. This matches the convention used by net/http middleware libraries
// (chi, gin, echo), gRPC interceptors, and Go's broader ecosystem.
//
// Registering the same Middleware multiple times runs it multiple times.
// Use must be called before worker.Start; registering middleware after the
// worker has begun handling invocations is undefined and not safe for
// concurrent use.
func (app *App) Use(mw Middleware) {
	if mw == nil {
		return
	}
	app.middlewares = append(app.middlewares, mw)
}

// Compose wraps inner with all registered Middleware and returns the resulting
// Handler. Middleware order matches Use's contract: the first registered
// Middleware is outermost and runs first/last around the chain.
//
// Compose is the only sanctioned way for code outside the sdk package (notably
// the worker dispatcher and the HTTP-streaming bridge) to obtain the
// per-invocation chain. The raw middleware slice is intentionally not
// exported; this keeps the registration list internal so it can evolve
// (for example, to introduce a separate framework-middleware pool) without
// breaking external consumers.
//
// Compose returns inner unchanged when no Middleware are registered.
func (app *App) Compose(inner Handler) Handler {
	for i := len(app.middlewares) - 1; i >= 0; i-- {
		inner = app.middlewares[i](inner)
	}
	return inner
}
