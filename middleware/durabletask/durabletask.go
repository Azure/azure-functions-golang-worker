package durabletask

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/microsoft/durabletask-go/task"
)

// Durable is the Durable Functions middleware. It implements
// [sdk.Middleware] and [sdk.FunctionProvider]: Wrap intercepts orchestration
// invocations and replays them, while ProvidedFunctions contributes the
// orchestrator and activity functions to the App so the host dispatches them
// to the worker.
//
// Construct it with [Middleware] and register work via options or the
// chainable [Durable.Orchestrator] / [Durable.Activity] methods, then enable
// it with App.Use.
type Durable struct {
	registry *task.TaskRegistry
	runner   *orchestrationRunner
	provided []sdk.FunctionRegistration

	// client is attached to the context of non-orchestration invocations so
	// HTTP starter functions can reach it via [ClientFromContext].
	client *Client
	// ownsClient is true when the middleware created the client (via
	// EnvGrpcEndpoint) and is therefore responsible for closing it on
	// Shutdown. Clients supplied via WithClient are the caller's to close.
	ownsClient bool

	// mu guards bindingClients.
	mu sync.Mutex
	// bindingClients caches the [Client] created for each durable gRPC
	// endpoint the host delivers via the durable client binding, keyed by gRPC
	// target. Reused across invocations and closed on Shutdown.
	bindingClients map[string]*Client
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
func WithActivity(name string, fn any) Option {
	return func(d *Durable) { d.Activity(name, fn) }
}

// WithClient sets the [Client] the middleware attaches to the context of
// non-orchestration invocations (so HTTP starters can reach it via
// [ClientFromContext]). When unset, the middleware dials [EnvGrpcEndpoint]
// if it is set; otherwise no client is attached and [ClientFromContext]
// returns false (fine when only orchestrator/activity execution is needed).
func WithClient(c *Client) Option {
	return func(d *Durable) { d.client = c }
}

// Middleware constructs the Durable Functions middleware.
//
//	app.Use(durabletask.Middleware(
//	    durabletask.WithOrchestrator("HelloCities", HelloCities),
//	    durabletask.WithActivity("SayHello", SayHello),
//	))
func Middleware(opts ...Option) *Durable {
	r := task.NewTaskRegistry()
	d := &Durable{
		registry: r,
		runner:   &orchestrationRunner{registry: r},
	}
	for _, opt := range opts {
		opt(d)
	}
	if d.client == nil {
		d.client = defaultClientFromEnv()
		d.ownsClient = d.client != nil
	}
	return d
}

// Orchestrator registers an orchestrator function under name and returns the
// receiver for chaining. The orchestrator uses the durabletask-go
// programming model (task.OrchestrationContext: CallActivity, CreateTimer,
// WaitForExternalEvent, …).
//
// Panics if name is already registered.
func (d *Durable) Orchestrator(name string, o task.Orchestrator) *Durable {
	if err := d.registry.AddOrchestratorN(name, o); err != nil {
		panic(fmt.Sprintf("durabletask: register orchestrator %q: %v", name, err))
	}
	d.provided = append(d.provided, sdk.FunctionRegistration{
		Name:    name,
		Func:    orchestrationPlaceholder,
		Trigger: orchestrationTriggerBinding{},
	})
	return d
}

// Activity registers an activity function under name and returns the
// receiver for chaining.
//
// An activity is an ordinary function with one of these shapes:
//
//	func(context.Context, In) (Out, error)
//	func(context.Context, In) error
//	func(context.Context) (Out, error)
//	func(context.Context) error
//
// It runs through the normal worker pipeline: In is deserialized from the
// invocation input and Out is encoded into the response. Activities are not
// replayed, so ordinary (non-deterministic) code is fine inside them.
//
// Panics if fn is not a valid activity signature.
func (d *Durable) Activity(name string, fn any) *Durable {
	validateActivity(name, fn)
	d.provided = append(d.provided, sdk.FunctionRegistration{
		Name:    name,
		Func:    fn,
		Trigger: activityTriggerBinding{},
	})
	return d
}

// Wrap implements [sdk.Middleware]. For orchestration invocations it replays
// the orchestrator and records the response, short-circuiting the chain so
// the registered placeholder never runs. For every other trigger it attaches
// the durable [Client] to the context and calls next.
func (d *Durable) Wrap(next sdk.Handler) sdk.Handler {
	return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
		if mc != nil && mc.TriggerType == string(OrchestrationTriggerType) {
			encodedResponse, err := d.runner.loadAndRun(ctx, string(mc.InputBytes()))
			if err != nil {
				return err
			}
			// The orchestrator's return value is the base64-encoded set of
			// actions; the worker encodes it into InvocationResponse.ReturnValue
			// and the host's DurableTask extension applies it.
			mc.SetReturnValue(encodedResponse)
			return nil
		}
		// Non-orchestration invocations (activities, HTTP starters, timers)
		// run normally. Attach a durable client so starters can reach it via
		// ClientFromContext. Prefer the endpoint the host delivered via the
		// durable client binding; fall back to the env/explicit client.
		if c := d.clientFromBinding(mc); c != nil {
			ctx = contextWithClient(ctx, c)
		} else if d.client != nil {
			ctx = contextWithClient(ctx, d.client)
		}
		return next(ctx, mc)
	}
}

// clientFromBinding returns a durable [Client] for the gRPC endpoint the host
// delivered via the durable client binding (see [ClientInput]), or nil when no
// such binding was present or its payload is unusable. Clients are created
// once per endpoint and reused.
func (d *Durable) clientFromBinding(mc *sdk.MiddlewareContext) *Client {
	if mc == nil {
		return nil
	}
	raw, ok := mc.BindingInput(durableClientParamName)
	if !ok || len(raw) == 0 {
		return nil
	}
	var data struct {
		RpcBaseURL string `json:"rpcBaseUrl"`
	}
	if err := json.Unmarshal(raw, &data); err != nil || data.RpcBaseURL == "" {
		return nil
	}
	return d.cachedClient(data.RpcBaseURL)
}

// cachedClient dials the gRPC endpoint behind rpcBaseURL once and reuses the
// resulting [Client] for subsequent invocations carrying the same endpoint.
// Returns nil if the connection cannot be established.
func (d *Durable) cachedClient(rpcBaseURL string) *Client {
	target := grpcTarget(rpcBaseURL)
	d.mu.Lock()
	defer d.mu.Unlock()
	if c, ok := d.bindingClients[target]; ok {
		return c
	}
	c, err := Dial(target)
	if err != nil {
		return nil
	}
	if d.bindingClients == nil {
		d.bindingClients = make(map[string]*Client, 1)
	}
	d.bindingClients[target] = c
	return c
}

// ProvidedFunctions implements [sdk.FunctionProvider]. It returns the
// orchestrator and activity functions registered on this middleware so
// App.Use registers them with the App.
func (d *Durable) ProvidedFunctions() []sdk.FunctionRegistration {
	return d.provided
}

// Shutdown implements [sdk.ShutdownProvider]. It closes the durable client's
// connection if the middleware created it (via [EnvGrpcEndpoint]) and closes
// every client created for a host-delivered durable client binding endpoint. A
// client supplied through [WithClient] is the caller's to close.
func (d *Durable) Shutdown(ctx context.Context) error {
	var firstErr error
	if d.client != nil && d.ownsClient {
		if err := d.client.Close(); err != nil {
			firstErr = err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range d.bindingClients {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	d.bindingClients = nil
	return firstErr
}

// orchestrationPlaceholder is the registered body of every orchestrator
// function. It never executes: [Durable.Wrap] short-circuits orchestration
// invocations and produces the response via replay. It exists only so the
// host receives function metadata and the worker can load the function. The
// signature takes the base64 history as []byte to match the orchestration
// trigger's single input binding.
func orchestrationPlaceholder(ctx context.Context, _ []byte) error { return nil }

var (
	ctxType = reflect.TypeOf((*context.Context)(nil)).Elem()
	errType = reflect.TypeOf((*error)(nil)).Elem()
)

// validateActivity panics if fn is not a valid activity signature:
// func(context.Context[, In]) ([Out, ]error).
func validateActivity(name string, fn any) {
	ft := reflect.TypeOf(fn)
	if ft == nil || ft.Kind() != reflect.Func {
		panic(fmt.Sprintf("durabletask: activity %q must be a function", name))
	}
	if ft.NumIn() < 1 || ft.NumIn() > 2 {
		panic(fmt.Sprintf("durabletask: activity %q must accept (context.Context) or (context.Context, In), got %d args", name, ft.NumIn()))
	}
	if !ft.In(0).Implements(ctxType) {
		panic(fmt.Sprintf("durabletask: activity %q first argument must be context.Context, got %v", name, ft.In(0)))
	}
	if ft.NumOut() < 1 || ft.NumOut() > 2 {
		panic(fmt.Sprintf("durabletask: activity %q must return (error) or (Out, error), got %d results", name, ft.NumOut()))
	}
	if !ft.Out(ft.NumOut() - 1).Implements(errType) {
		panic(fmt.Sprintf("durabletask: activity %q last return value must be error, got %v", name, ft.Out(ft.NumOut()-1)))
	}
}
