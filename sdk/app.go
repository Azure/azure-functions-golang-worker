package sdk

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
)

// Option is a functional option applied to a RegisteredFunction during registration.
type Option func(*RegisteredFunction)

// App represents the function application and its registered functions.
type App struct {
	registeredFunctions *sync.Map
	// middlewares are appended in registration order. Composed by the worker
	// dispatcher into the per-invocation chain. See [App.Use] for ordering
	// semantics.
	middlewares []Middleware
	// capabilities is the merged capability map collected from every
	// [Middleware] that implements [CapabilityProvider] at registration
	// time. The worker dispatcher copies this into
	// WorkerInitResponse.Capabilities. See [App.Capabilities].
	capabilities map[string]string
	// shutdowns are cleanup callbacks contributed by middlewares that
	// implement [ShutdownProvider]. The worker invokes them sequentially
	// in registration order after the gRPC stream closes; see
	// [App.RunShutdowns].
	shutdowns []func(context.Context) error
}

// FunctionApp creates a new App instance.
func FunctionApp() *App {
	return &App{
		registeredFunctions: &sync.Map{},
	}
}

// GetRegisteredFunctions returns the registered functions map.
// This is used internally by the worker.
func (app *App) GetRegisteredFunctions() *sync.Map {
	return app.registeredFunctions
}

// --- Trigger Registration ---

// HTTP creates a new HTTP triggered function.
func (app *App) HTTP(name string, f HTTPHandler, opts ...Option) *RegisteredFunction {
	trigger := &bindings.HTTPTrigger{
		Name:      "req",
		Route:     name,
		AuthLevel: "anonymous",
		Methods:   []string{http.MethodGet, http.MethodPost},
	}
	return app.registerFunction(name, f, trigger, opts...)
}

// Timer creates a new timer-triggered function.
func (app *App) Timer(name string, f TimerHandler, opts ...Option) *RegisteredFunction {
	trigger := &bindings.TimerTrigger{
		Name: "timer",
	}
	return app.registerFunction(name, f, trigger, opts...)
}

// CosmosDB creates a new CosmosDB triggered function.
func (app *App) CosmosDB(name string, f CosmosDBHandler, opts ...Option) *RegisteredFunction {
	trigger := &bindings.CosmosDBTrigger{
		Name: "docs",
	}
	return app.registerFunction(name, f, trigger, opts...)
}

// EventGrid creates a new EventGrid triggered function.
func (app *App) EventGrid(name string, f EventGridHandler, opts ...Option) *RegisteredFunction {
	trigger := &bindings.EventGridTrigger{
		Name: "event",
	}
	return app.registerFunction(name, f, trigger, opts...)
}

// EventHub creates a new EventHub triggered function.
func (app *App) EventHub(name string, f EventHubHandler, opts ...Option) *RegisteredFunction {
	trigger := &bindings.EventHubTrigger{
		Name:          "message",
		ConsumerGroup: "$Default",
		Cardinality:   "one",
	}
	return app.registerFunction(name, f, trigger, opts...)
}

// ServiceBusQueue creates a new Service Bus queue triggered function.
func (app *App) ServiceBusQueue(name string, f ServiceBusHandler, opts ...Option) *RegisteredFunction {
	trigger := &bindings.ServiceBusQueueTrigger{
		Name:        "message",
		Cardinality: "one",
	}
	return app.registerFunction(name, f, trigger, opts...)
}

// ServiceBusTopic creates a new Service Bus topic triggered function.
func (app *App) ServiceBusTopic(name string, f ServiceBusHandler, opts ...Option) *RegisteredFunction {
	trigger := &bindings.ServiceBusTopicTrigger{
		Name:        "message",
		Cardinality: "one",
	}
	return app.registerFunction(name, f, trigger, opts...)
}

// =============================================================================
// Extension Triggers — SDK Client Injection
//
// These triggers provide an authenticated Azure SDK client instead of raw data.
// The host sends only metadata (e.g., container name, blob path); the actual
// content never flows through gRPC.
//
// Extension triggers have:
//   - SDK client injection — handler receives e.g. *blob.Client ready to use
//   - Isolated dependencies — heavy Azure SDK deps live in triggers/<name>/
//   - ClientFactory registration — activated via blank import + init()
//   - Handler type is 'any' (validated via reflection at registration time)
//
// This tier is used when the trigger payload is potentially unbounded (GBs)
// or when a useful handler requires a live SDK client for streaming, random
// access, or write-back operations.
//
// Triggers in this tier: Blob (and future Queue, Table, etc.)
//
// See sdk/factories.go for the ClientFactory mechanism and decision criteria.
// =============================================================================

// --- Blob Trigger ---

// Blob creates a new blob-triggered function.
// The handler argument type depends on the registered blob trigger extension.
// Import the triggers/blob package to enable *blob.Client support:
//
// import _ "github.com/azure/azure-functions-golang-worker/triggers/blob"
func (app *App) Blob(name string, f any, opts ...Option) *RegisteredFunction {
	// Validate handler signature: must be a function
	ft := reflect.TypeOf(f)
	if ft == nil || ft.Kind() != reflect.Func {
		panic("Blob handler must be a function")
	}
	// Must accept exactly 2 args: (context.Context, T)
	if ft.NumIn() != 2 {
		panic(fmt.Sprintf("Blob handler must accept exactly 2 arguments (context.Context, clientType), got %d", ft.NumIn()))
	}
	// First arg must implement context.Context
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	if !ft.In(0).Implements(ctxType) {
		panic(fmt.Sprintf("Blob handler first argument must be context.Context, got %v", ft.In(0)))
	}
	// Must return exactly 1 value: error
	if ft.NumOut() != 1 {
		panic(fmt.Sprintf("Blob handler must return exactly 1 value (error), got %d", ft.NumOut()))
	}
	errType := reflect.TypeOf((*error)(nil)).Elem()
	if !ft.Out(0).Implements(errType) {
		panic(fmt.Sprintf("Blob handler return type must be error, got %v", ft.Out(0)))
	}

	trigger := &bindings.BlobTrigger{
		Name: "blob",
	}

	rf := app.registerFunction(name, f, trigger, opts...)

	// Look up the globally registered factory for blob triggers
	if factory, ok := GetClientFactory(string(bindings.BlobTriggerType)); ok {
		rf.ClientFactory = factory
	} else {
		slog.LogAttrs(context.Background(), slog.LevelWarn,
			"no ClientFactory registered for blob trigger; did you forget to import triggers/blob?",
			slog.String("trigger_type", string(bindings.BlobTriggerType)),
			slog.String("function_name", name),
		)
	}

	return rf
}

// --- Registration ---

// RegisteredFunction holds metadata about a registered function.
type RegisteredFunction struct {
	Func          any
	FuncName      string
	FuncId        string
	RawBindings   []bindings.Binding
	Retry         *RetryOptions
	ScriptFile    string
	TriggerType   string
	ClientFactory ClientFactory // Optional: creates trigger-specific client args
}

// RegisterFunction registers a function with an explicit name and a trigger binding.
// This is exported for use by external trigger modules (e.g., triggers/blob).
func (app *App) RegisterFunction(name string, f any, b bindings.Bind, opts ...Option) *RegisteredFunction {
	return app.registerFunction(name, f, b, opts...)
}

func (app *App) registerFunction(name string, f any, b bindings.Bind, opts ...Option) *RegisteredFunction {
	triggerBinding := b.ToBinding()
	rawBindings := []bindings.Binding{triggerBinding}

	// If this is an HTTP Trigger, we implicitly add the HTTP Output binding
	if b.GetBindingType() == bindings.HTTPTriggerType {
		rawBindings = append(rawBindings, bindings.Binding{
			Name:      "$return",
			Type:      "http",
			Direction: "out",
		})
	}

	ptr := reflect.ValueOf(f).Pointer()
	fun := runtime.FuncForPC(ptr)
	file, _ := fun.FileLine(ptr)

	// Use the explicit name if provided, otherwise derive from function
	funcName := name
	if funcName == "" {
		funcName = GetFunctionName(f)
	}

	rf := &RegisteredFunction{
		Func:        f,
		FuncName:    funcName,
		ScriptFile:  file,
		RawBindings: rawBindings,
		TriggerType: string(b.GetBindingType()),
	}

	// Apply all options before storing
	for _, opt := range opts {
		opt(rf)
	}

	funcId, err := HashFunctionID(*rf)
	if err != nil {
		panic(err)
	}

	rf.FuncId = funcId
	if existing, loaded := app.registeredFunctions.LoadOrStore(funcId, rf); loaded {
		existingFunc := existing.(*RegisteredFunction)
		panic(fmt.Sprintf("duplicate function registration: %q (trigger type %q) conflicts with existing function %q",
			rf.FuncName, rf.TriggerType, existingFunc.FuncName))
	}
	return rf
}

// GetFunctionName returns the simple name of the function, stripping the package path.
func GetFunctionName(f any) string {
	fullName := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	parts := strings.Split(fullName, ".")
	return parts[len(parts)-1]
}

// HashFunctionID generates a unique ID for the function based on its name
// and trigger type to avoid collisions between functions with the same name.
func HashFunctionID(rf RegisteredFunction) (string, error) {
	var sb strings.Builder
	sb.WriteString(rf.FuncName)
	sb.WriteString(":")
	sb.WriteString(rf.TriggerType)

	hash := sha256.New()
	if _, err := hash.Write([]byte(sb.String())); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
