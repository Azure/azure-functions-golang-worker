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

// Middlewares returns the registered middleware slice. The returned slice
// shares storage with the App; callers must not mutate it. Used by the
// worker dispatcher to compose the per-invocation chain.
func (app *App) Middlewares() []Middleware {
	return app.middlewares
}

// ComposeMiddleware wraps inner with mws in registration order: mws[0] is
// outermost. Returns inner unchanged if mws is empty.
//
// Exposed for the worker dispatcher (and any equivalent integration point,
// e.g. the HTTP-streaming proxy) to build the per-invocation chain. User
// code does not normally call ComposeMiddleware directly.
func ComposeMiddleware(mws []Middleware, inner Handler) Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		inner = mws[i](inner)
	}
	return inner
}
