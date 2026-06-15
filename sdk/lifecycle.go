package sdk

import "context"

// LifecycleHook is a process-lifetime resource whose Start and Shutdown are
// owned by worker.Start.
//
// Start is invoked once before the worker begins serving invocations and
// should block until the resource is ready to use (or return an error). A
// non-nil error from Start is fatal: the worker logs it and terminates. Hooks
// that prefer to degrade gracefully on failure should log internally and
// return nil instead.
//
// Shutdown is invoked once during worker teardown (after the gRPC stream
// closes or a termination signal is received) so the resource can flush and
// release. It observes the supplied ctx, which the worker plumbs with a
// bounded timeout.
//
// The interface is intentionally decoupled from any concrete implementation so
// the worker package never imports heavyweight dependencies (for example, an
// embedded OpenTelemetry Collector). Producers wrap a hook as a [StartOption]
// via [WithLifecycleHook].
type LifecycleHook interface {
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

// LifecycleProvider is an optional contract a [Middleware] can implement to
// contribute a [LifecycleHook] at registration time.
//
// When a Middleware is registered via [App.Use], the App checks whether it
// satisfies LifecycleProvider; if so, the returned hook is collected and the
// worker starts it before serving and shuts it down at teardown — exactly as
// if it had been passed to worker.Start via [WithLifecycleHook]. This lets a
// single App.Use call fully wire a middleware that also needs a
// process-lifetime resource (for example, a background listener), so the user
// does not have to register the hook separately.
//
// Lifecycle is read once at registration time and should return a stable hook
// (returning nil contributes nothing). Keeping the hook a distinct value from
// the Middleware avoids the Middleware accidentally satisfying
// [ShutdownProvider] through the hook's Shutdown method.
type LifecycleProvider interface {
	Lifecycle() LifecycleHook
}

// StartConfig accumulates the options passed to worker.Start. It is populated
// by applying each [StartOption] and consumed by the worker package.
type StartConfig struct {
	// Hooks are the lifecycle hooks to start before serving and shut down
	// during teardown, in registration order.
	Hooks []LifecycleHook
}

// StartOption configures worker.Start. Options are applied in the order they
// are passed.
type StartOption func(*StartConfig)

// WithLifecycleHook registers a [LifecycleHook] to be started before the
// worker serves and shut down during teardown. A nil hook is silently ignored
// so callers can register conditional hooks without an explicit guard.
//
// This is the sanctioned bridge for packages outside sdk (for example,
// otelcollector) to participate in the worker lifecycle without the worker
// importing them.
func WithLifecycleHook(h LifecycleHook) StartOption {
	return func(c *StartConfig) {
		if h != nil {
			c.Hooks = append(c.Hooks, h)
		}
	}
}
