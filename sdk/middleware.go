// Package sdk provides the core function-app and middleware abstractions for
// the Azure Functions Go worker. This file documents the middleware design
// intent.
//
// # Middleware extensibility
//
// The Middleware interface (Wrap(next Handler) -> Handler) is deliberately
// minimal, matching the shape established by net/http (Handler/HandlerFunc)
// and gRPC interceptors. It supports the full range of cross-cutting
// concerns: distributed tracing, structured logging, authentication, retry
// policies, panic recovery, and request/response validation.
//
// Middleware that wants to replace function execution entirely (e.g.
// orchestration replay) can short-circuit the chain by skipping next();
// no separate extension point is provided today.
package sdk

import (
	"context"

	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
)

// Handler is the unit a Middleware wraps. It runs a single function invocation
// and returns nil on success or an error that becomes the failure status on
// the InvocationResponse.
//
// The MiddlewareContext passed in is the framework's per-invocation carrier.
// It embeds *InvocationContext so trigger-side fields (mc.InvocationID,
// mc.FunctionName, mc.TraceContext, etc.) are reachable directly via field
// promotion; middleware that only reads trigger data treats it like the
// user-facing InvocationContext. Middleware integration code (e.g.
// auto-harvested outbound trace attributes) uses the MC-specific methods on
// the same value.
//
// Note: Handler is the worker-internal function shape that middleware
// composes around. User-facing handler signatures (HTTPHandler, TimerHandler,
// etc.) remain unchanged; the dispatcher constructs the innermost Handler
// from the user's typed function via reflection.
type Handler func(ctx context.Context, mc *MiddlewareContext) error

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
type Middleware interface {
	// Wrap returns a Handler that decorates next.
	Wrap(next Handler) Handler
}

// MiddlewareFunc adapts a plain function of the shape
// `func(next Handler) Handler` to the [Middleware] interface. Use it when
// no per-instance state is needed:
//
//	app.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
//	    return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
//	        log.Printf("invocation %s", mc.InvocationID)
//	        return next(ctx, mc)
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

// ShutdownProvider is an optional contract a [Middleware] can implement when
// it owns asynchronous resources (TracerProviders, LoggerProviders, exporter
// batch processors, etc.) that need to be flushed and released when the
// worker process is shutting down.
//
// When a Middleware is registered via [App.Use], the App checks whether it
// satisfies ShutdownProvider; if so, the returned callback is appended to the
// App's shutdown list. The worker invokes those callbacks sequentially in
// registration order once the gRPC bidi stream closes (graceful host
// termination) or the process receives a SIGTERM/SIGINT.
//
// Callbacks should respect ctx (which the worker plumbs with a bounded
// timeout) and propagate any unrecoverable error so the worker can log it
// before exiting. Returning nil is the success path. Callbacks must be safe
// to invoke at most once.
//
// The intent is that user code does not need a defer cleanup() line in
// main(); the worker's lifecycle owns the shutdown sequence and middlewares
// own the resources they create.
type ShutdownProvider interface {
	Shutdown(ctx context.Context) error
}

// FunctionRegistration describes a single function a [FunctionProvider]
// contributes to the App. The fields mirror the arguments of
// [App.RegisterFunction]: a name, the handler, the trigger binding, and any
// registration options.
type FunctionRegistration struct {
	// Name is the function name (and, for durable triggers, the
	// orchestration / activity name the host dispatches by).
	Name string
	// Func is the handler. Its accepted shapes are the same as any other
	// registered function; a middleware that replaces execution may pass a
	// placeholder whose body never runs (the middleware short-circuits the
	// chain and produces the response itself).
	Func any
	// Trigger is the binding that drives the function.
	Trigger bindings.Bind
	// Options are applied during registration (e.g. retry policy).
	Options []Option
}

// FunctionProvider is an optional contract a [Middleware] can implement to
// contribute functions to the App at registration time.
//
// When a Middleware is registered via [App.Use], the App checks whether it
// satisfies FunctionProvider; if so, each returned [FunctionRegistration]
// is registered exactly as if the user had called [App.RegisterFunction].
// This lets a self-contained middleware own both the cross-cutting behavior
// (via Wrap) and the function declarations it depends on, so the user wires
// the whole feature with a single App.Use call.
//
// The motivating consumer is durable functions: a single
// durabletask.Middleware() both intercepts orchestration invocations (Wrap)
// and declares the orchestrator / activity / client functions the host must
// know about (ProvidedFunctions), so the host emits metadata for them and
// dispatches them to the worker.
//
// ProvidedFunctions is read once at registration time and must be
// side-effect-free: callers other than [App.Use] may invoke it to inspect what
// a middleware contributes, and doing so must not change the middleware's
// behavior. A provider that needs to know when the App has consumed its
// registrations should implement [RegistrationSealer] instead.
type FunctionProvider interface {
	ProvidedFunctions() []FunctionRegistration
}

// RegistrationSealer is an optional contract a [FunctionProvider] can
// implement to be told that [App.Use] has consumed its registrations.
//
// App.Use reads [FunctionProvider.ProvidedFunctions] exactly once, at the
// moment it is called. Anything a provider registers afterwards is never seen
// by the App, so the corresponding function is never indexed by the host and
// simply never runs. A provider that can detect this should implement
// SealRegistration and reject late registrations with a clear error rather
// than accepting them into a registry nothing will read.
//
// SealRegistration is called once App.Use has taken the provider's
// registrations, before they are applied to the App. Sealing first means a
// failure while applying them cannot leave the provider open and still
// accepting registrations nothing will read.
//
// It may be called more than once if the same middleware is passed to
// App.Use repeatedly, so implementations should be idempotent.
type RegistrationSealer interface {
	SealRegistration()
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
// If mw also implements [ShutdownProvider], its Shutdown method is registered
// for invocation by the worker once the gRPC stream closes or the process
// receives a termination signal. Shutdowns run in registration order.
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

	if cp, ok := mw.(CapabilityProvider); ok {
		caps := cp.Capabilities()
		if len(caps) > 0 {
			if app.capabilities == nil {
				app.capabilities = make(map[string]string, len(caps))
			}
			for k, v := range caps {
				app.capabilities[k] = v
			}
		}
	}

	if sp, ok := mw.(ShutdownProvider); ok {
		app.shutdowns = append(app.shutdowns, sp.Shutdown)
	}

	if fp, ok := mw.(FunctionProvider); ok {
		provided := fp.ProvidedFunctions()

		// Seal before applying them. Sealing afterwards would leave the
		// provider open if registerFunction panicked partway through, and it
		// would then keep accepting registrations nothing will ever read.
		if rs, ok := mw.(RegistrationSealer); ok {
			rs.SealRegistration()
		}

		for _, fr := range provided {
			app.registerFunction(fr.Name, fr.Func, fr.Trigger, fr.Options...)
		}
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
// Compose is the only sanctioned way for code outside the sdk package
// (the worker dispatcher and the HTTP-streaming bridge) to obtain the
// per-invocation chain.
//
// Compose returns inner unchanged when no Middleware are registered.
func (app *App) Compose(inner Handler) Handler {
	for i := len(app.middlewares) - 1; i >= 0; i-- {
		inner = app.middlewares[i].Wrap(inner)
	}
	return inner
}

// RunShutdowns invokes every registered [ShutdownProvider]'s Shutdown
// callback in registration order, returning the first error encountered (or
// nil if every callback succeeded).
//
// The worker calls RunShutdowns after the gRPC stream closes or the process
// receives a termination signal. Callbacks observe the supplied ctx so
// timeouts (typically a few seconds, bounded by the worker shutdown deadline)
// propagate to in-flight flushes and exporter draining.
//
// User code does not normally call RunShutdowns; it is exposed for the worker
// (and tests) and is a no-op when no middleware contributed a Shutdown.
func (app *App) RunShutdowns(ctx context.Context) error {
	var firstErr error
	for _, fn := range app.shutdowns {
		if err := fn(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
