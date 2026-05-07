// Package sdk provides the core function-app and middleware abstractions for
// the Azure Functions Go worker. This file documents the middleware design
// intent.
//
// # Middleware extensibility
//
// The Middleware interface (Wrap(next Handler) -> Handler) is deliberately
// minimal, matching the shape established by net/http (Handler/HandlerFunc),
// gRPC interceptors, and aws-lambda-go's otellambda. It supports the full
// range of cross-cutting concerns: distributed tracing, structured logging,
// authentication, retry policies, panic recovery, and request/response
// validation.
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
// Wrap receives the next Handler in the chain and returns a new Handler that
// may run code before/after calling next, may transform ctx (for example, by
// attaching an OpenTelemetry span), or may short-circuit by returning without
// calling next.
//
// The interface shape (rather than a function type) is intentional: it lets
// implementations carry per-instance state (e.g. a TracerProvider, an
// authentication policy) and lets them participate in optional contracts such
// as [CapabilityProvider] without forcing every middleware to be the same
// concrete type. Plain function middleware can wrap itself in [MiddlewareFunc]
// to satisfy the interface — exactly the net/http Handler/HandlerFunc pattern.
//
// Third-party modules can author and ship Middleware implementations against
// this interface without any coordination with the worker; the bundled
// middleware/otelfunc package is itself a regular consumer of this API.
type Middleware interface {
	// Wrap returns a Handler that decorates next.
	Wrap(next Handler) Handler
}

// MiddlewareFunc adapts a plain function of the shape
// `func(next Handler) Handler` to the [Middleware] interface. Use it when
// no per-instance state is needed:
//
//	app.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
//	    return func(ctx context.Context, ic *sdk.InvocationContext) error {
//	        log.Printf("invocation %s", ic.InvocationID)
//	        return next(ctx, ic)
//	    }
//	}))
//
// Mirrors the net/http Handler/HandlerFunc pairing.
type MiddlewareFunc func(next Handler) Handler

// Wrap implements [Middleware].
func (f MiddlewareFunc) Wrap(next Handler) Handler {
	return f(next)
}

// CapabilityProvider is an optional contract a [Middleware] can implement to
// advertise worker-level capability flags to the Functions host.
//
// When a Middleware is registered via [App.Use], the App checks whether it
// satisfies CapabilityProvider; if so, the returned capability map is merged
// into the App's capability map, which the worker dispatcher copies into the
// WorkerInitResponse.Capabilities field so the host knows what the worker
// supports.
//
// The standard use case is a tracing middleware advertising
// "WorkerOpenTelemetryEnabled": "true" so the host knows the worker is
// emitting OTel telemetry directly and shouldn't double-emit to Application
// Insights for the same invocation.
//
// Implementations should return a stable, side-effect-free map. The App reads
// it once at registration time.
type CapabilityProvider interface {
	Capabilities() map[string]string
}

// Use registers a [Middleware]. Middleware run in registration order: the
// first registered is the outermost — i.e. it observes the invocation first
// and last. This matches the convention used by net/http middleware libraries
// (chi, gin, echo), gRPC interceptors, and Go's broader ecosystem.
//
// If mw also implements [CapabilityProvider], its capability map is merged
// into the App's capability map at registration time. Later registrations
// overwrite earlier values for the same key; deterministic behavior is the
// registrant's responsibility.
//
// Registering the same Middleware multiple times runs its Wrap multiple times.
// Use must be called before worker.Start; registering middleware after the
// worker has begun handling invocations is undefined and not safe for
// concurrent use.
//
// A nil mw is silently dropped, so callers can register conditional
// middleware without an explicit guard:
//
//	app.Use(maybeMiddleware()) // safe even when maybeMiddleware returns nil
func (app *App) Use(mw Middleware) {
	if mw == nil {
		return
	}
	app.middlewares = append(app.middlewares, mw)

	cp, ok := mw.(CapabilityProvider)
	if !ok {
		return
	}
	caps := cp.Capabilities()
	if len(caps) == 0 {
		return
	}
	if app.capabilities == nil {
		app.capabilities = make(map[string]string, len(caps))
	}
	for k, v := range caps {
		app.capabilities[k] = v
	}
}

// Capabilities returns the merged capability map advertised by every
// registered [Middleware] that implements [CapabilityProvider]. The worker
// dispatcher copies the result into WorkerInitResponse.Capabilities so the
// host is informed of worker-level features (e.g. native OpenTelemetry
// emission).
//
// The returned map is a shallow copy and is safe for the caller to mutate
// or retain. An empty map (rather than nil) is returned when no capabilities
// are registered, simplifying caller code that wants to range over the
// result.
func (app *App) Capabilities() map[string]string {
	out := make(map[string]string, len(app.capabilities))
	for k, v := range app.capabilities {
		out[k] = v
	}
	return out
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
		inner = app.middlewares[i].Wrap(inner)
	}
	return inner
}
