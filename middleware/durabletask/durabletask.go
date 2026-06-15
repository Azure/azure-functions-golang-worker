package durabletask

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Durable is the Durable Functions integration for the Go worker, built on the
// "model 2" (gRPC work-item stream) execution model.
//
// In this model the Functions host's DurableTask extension owns all durable
// state and exposes a local gRPC sidecar (the TaskHubSidecarService). The
// worker plays two roles over that connection:
//
//   - a work-item listener that pulls orchestrator/activity work items and
//     executes them via durabletask-go, and
//   - a management client that starter functions use to schedule and manage
//     instances.
//
// Both halves speak durabletask-go's programming model
// (task.OrchestrationContext / task.ActivityContext), so the same
// orchestrator/activity functions are registered once and executed by the
// listener.
//
// Durable is an [sdk.Middleware] and a [sdk.LifecycleProvider], so a single
// App.Use call wires the whole feature — the Wrap injects the management
// client into starter invocations, and the contributed [sdk.LifecycleHook]
// starts/stops the work-item listener with the worker:
//
//	app := sdk.FunctionApp()
//	app.Use(durabletask.Middleware(
//	    durabletask.WithOrchestrator("HelloCities", HelloCities),
//	    durabletask.WithActivity("SayHello", SayHello),
//	))
//	app.HTTP("start", StartHelloCities, sdk.WithMethods("post"))
//	worker.Start(app) // listener starts automatically; no extra hook to register
type Durable struct {
	registry *task.TaskRegistry
	endpoint string
	logger   backend.Logger

	// conn is an externally supplied connection (tests / custom transport).
	// When nil, the listener hook dials endpoint and owns the connection.
	conn grpc.ClientConnInterface

	// hook is the stable [sdk.LifecycleHook] returned by Lifecycle. It is a
	// distinct value from Durable so that registering Durable via App.Use does
	// not also register Durable as a [sdk.ShutdownProvider] (the hook owns
	// Shutdown; Durable does not implement it).
	hook *listenerHook

	mu             sync.Mutex
	ownConn        *grpc.ClientConn
	client         *Client
	listenerCancel context.CancelFunc
}

// Option configures a [Durable] at construction time.
type Option func(*Durable)

// WithOrchestrator registers an orchestrator function under name. Equivalent
// to calling [Durable.Orchestrator] after construction.
func WithOrchestrator(name string, o task.Orchestrator) Option {
	return func(d *Durable) { d.Orchestrator(name, o) }
}

// WithActivity registers an activity function under name. Equivalent to
// calling [Durable.Activity] after construction.
func WithActivity(name string, a task.Activity) Option {
	return func(d *Durable) { d.Activity(name, a) }
}

// WithEndpoint sets the host DurableTask gRPC endpoint the listener and client
// connect to (e.g. "127.0.0.1:4001"). When unset, [Middleware] falls back to
// the EnvGrpcEndpoint environment variable.
func WithEndpoint(addr string) Option {
	return func(d *Durable) { d.endpoint = addr }
}

// WithConnection supplies an existing gRPC connection instead of dialing an
// endpoint. The caller owns the connection's lifecycle. Primarily for tests
// (e.g. an in-memory bufconn) or custom/TLS transports.
func WithConnection(conn grpc.ClientConnInterface) Option {
	return func(d *Durable) { d.conn = conn }
}

// WithLogger sets the durabletask-go logger used by the listener and client.
func WithLogger(l backend.Logger) Option {
	return func(d *Durable) { d.logger = l }
}

// Middleware constructs the Durable Functions integration. Register it with a
// single App.Use call; it injects the durable client into starter invocations
// and (via [sdk.LifecycleProvider]) contributes the work-item listener that
// the worker starts and stops automatically.
//
//	app.Use(durabletask.Middleware(
//	    durabletask.WithOrchestrator("HelloCities", HelloCities),
//	    durabletask.WithActivity("SayHello", SayHello),
//	))
func Middleware(opts ...Option) *Durable {
	d := &Durable{
		registry: task.NewTaskRegistry(),
		endpoint: os.Getenv(EnvGrpcEndpoint),
		logger:   backend.DefaultLogger(),
	}
	for _, opt := range opts {
		opt(d)
	}
	d.hook = &listenerHook{d: d}
	return d
}

// Orchestrator registers an orchestrator function under name and returns the
// receiver for chaining. The orchestrator uses the durabletask-go programming
// model (task.OrchestrationContext: CallActivity, CreateTimer,
// WaitForSingleEvent, SetCustomStatus, …) and must be deterministic.
//
// Panics if name is already registered.
func (d *Durable) Orchestrator(name string, o task.Orchestrator) *Durable {
	if err := d.registry.AddOrchestratorN(name, o); err != nil {
		panic(fmt.Sprintf("durabletask: register orchestrator %q: %v", name, err))
	}
	return d
}

// Activity registers an activity function under name and returns the receiver
// for chaining. An activity has the durabletask-go signature
// func(task.ActivityContext) (any, error); it reads input with
// ctx.GetInput(&v) and returns its result. Activities are not replayed, so
// ordinary (non-deterministic) code is fine inside them.
//
// Panics if name is already registered.
func (d *Durable) Activity(name string, a task.Activity) *Durable {
	if err := d.registry.AddActivityN(name, a); err != nil {
		panic(fmt.Sprintf("durabletask: register activity %q: %v", name, err))
	}
	return d
}

// Wrap implements [sdk.Middleware]. It attaches the durable [Client] to each
// invocation's context so HTTP starter functions can reach it via
// [ClientFromContext]. The client is read lazily at invocation time, after the
// listener hook's Start has established it.
func (d *Durable) Wrap(next sdk.Handler) sdk.Handler {
	return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
		if c := d.Client(); c != nil {
			ctx = contextWithClient(ctx, c)
		}
		return next(ctx, mc)
	}
}

// Lifecycle implements [sdk.LifecycleProvider]. It returns the [sdk.LifecycleHook]
// that runs the work-item listener for the worker's serving lifetime. App.Use
// collects it so the worker starts and stops it automatically — the user does
// not register it separately.
func (d *Durable) Lifecycle() sdk.LifecycleHook {
	return d.hook
}

// Client returns the management [Client] once the listener hook's Start has
// run, or nil before (or when durable is inactive). Starter functions normally
// reach it through [ClientFromContext] instead.
func (d *Durable) Client() *Client {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.client
}

// listenerHook is the [sdk.LifecycleHook] that owns the durable connection and
// work-item listener. It is separate from [Durable] so that registering
// Durable as a middleware does not also make it a [sdk.ShutdownProvider].
type listenerHook struct{ d *Durable }

// Start establishes the durable gRPC connection, builds the management
// [Client], and starts the work-item listener that executes registered
// orchestrators and activities.
//
// When no endpoint and no connection are configured, Start logs and returns
// nil so a worker can run its non-durable functions without a durable host
// present (durable simply stays inactive).
func (h *listenerHook) Start(ctx context.Context) error {
	d := h.d
	d.mu.Lock()
	defer d.mu.Unlock()

	conn := d.conn
	if conn == nil {
		if d.endpoint == "" {
			slog.WarnContext(ctx, "durabletask: no endpoint configured; durable functions inactive",
				slog.String("env", EnvGrpcEndpoint))
			return nil
		}
		gc, err := grpc.NewClient(d.endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("durabletask: dial %q: %w", d.endpoint, err)
		}
		d.ownConn = gc
		conn = gc
	}

	d.client = newClient(conn, d.logger)

	// The listener runs for the worker's serving lifetime. Use a dedicated
	// cancelable context so Shutdown can stop it deterministically.
	lctx, cancel := context.WithCancel(context.Background())
	d.listenerCancel = cancel
	if err := d.client.startWorkItemListener(lctx, d.registry); err != nil {
		cancel()
		d.listenerCancel = nil
		if d.ownConn != nil {
			_ = d.ownConn.Close()
			d.ownConn = nil
		}
		return fmt.Errorf("durabletask: start work-item listener: %w", err)
	}
	return nil
}

// Shutdown stops the work-item listener and closes the connection if Start
// dialed it.
func (h *listenerHook) Shutdown(ctx context.Context) error {
	d := h.d
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.listenerCancel != nil {
		d.listenerCancel()
		d.listenerCancel = nil
	}
	if d.ownConn != nil {
		err := d.ownConn.Close()
		d.ownConn = nil
		return err
	}
	return nil
}

// compile-time interface assertions.
var (
	_ sdk.Middleware        = (*Durable)(nil)
	_ sdk.LifecycleProvider = (*Durable)(nil)
	_ sdk.LifecycleHook     = (*listenerHook)(nil)
)
