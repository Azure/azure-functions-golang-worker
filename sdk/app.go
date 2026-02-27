package sdk

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
)

// App represents the function application and its registered functions.
type App struct {
	RegisteredFunctions *sync.Map
}

// FunctionApp creates a new App instance.
func FunctionApp() *App {
	return &App{
		RegisteredFunctions: &sync.Map{}, // string -> RegisteredFunction
	}
}

// HttpFunctionBuilder is a builder for creating HTTP triggered functions.
type HttpFunctionBuilder struct {
	trigger *bindings.HttpTrigger
	rf      *RegisteredFunction
}

// HTTP creates a new HTTP triggered function.
func (app *App) HTTP(name string, f interface{}) *HttpFunctionBuilder {
	trigger := &bindings.HttpTrigger{
		Name:      "req",
		Route:     name,
		AuthLevel: "anonymous",
		Methods:   []string{"GET", "POST"},
	}

	rf := app.RegisterFunction(f, trigger)

	// Ensure the route matches the name initially requested if no route was set
	// Note: RegisterFunction already generated ID.
	// If we mutate trigger later, the ID must be stable OR we must update the key.
	// Current ID hash logic uses RawBindings from 'rf'.
	// RegisterFunction copies Trigger -> RawBindings immediately.
	// We need to change RegisteredFunction to hold reference to triggers.

	return &HttpFunctionBuilder{
		trigger: trigger,
		rf:      rf,
	}
}

// Methods sets the allowed HTTP methods.
func (b *HttpFunctionBuilder) Methods(methods ...string) *HttpFunctionBuilder {
	b.trigger.Methods = methods
	// Update the raw binding in the registered function
	// This is a hack because RegisteredFunction stores a COPY of the binding.
	// We should refactor RegisteredFunction to store the Source Bindings.
	// For now, let's update the copy.

	// Find the input binding (first one)
	if len(b.rf.RawBindings) > 0 {
		// Re-generate
		newBinding := b.trigger.ToBinding()
		b.rf.RawBindings[0] = newBinding
	}
	return b
}

// Auth sets the authorization level.
func (b *HttpFunctionBuilder) Auth(level string) *HttpFunctionBuilder {
	b.trigger.AuthLevel = level
	if len(b.rf.RawBindings) > 0 {
		newBinding := b.trigger.ToBinding()
		b.rf.RawBindings[0] = newBinding
	}
	return b
}

// BlobInput adds a blob input binding.
func (b *HttpFunctionBuilder) BlobInput(name, path, connection string) *HttpFunctionBuilder {
	blobInput := &bindings.BlobInput{
		Name:       name,
		Path:       path,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, blobInput.ToBinding())
	return b
}

// BlobOutput adds a blob output binding.
func (b *HttpFunctionBuilder) BlobOutput(name, path, connection string) *HttpFunctionBuilder {
	blobOutput := &bindings.BlobOutput{
		Name:       name,
		Path:       path,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, blobOutput.ToBinding())
	return b
}

// CosmosFunctionBuilder is a builder for creating CosmosDB triggered functions.
type CosmosFunctionBuilder struct {
	trigger *bindings.CosmosDB
	rf      *RegisteredFunction
}

// CosmosDB creates a new CosmosDB triggered function.
func (app *App) CosmosDB(name string, f interface{}) *CosmosFunctionBuilder {
	trigger := &bindings.CosmosDB{
		ArgName: "docs",
	}

	rf := app.RegisterFunction(f, trigger)

	return &CosmosFunctionBuilder{
		trigger: trigger,
		rf:      rf,
	}
}

// Database sets the CosmosDB database name.
func (b *CosmosFunctionBuilder) Database(dbName string) *CosmosFunctionBuilder {
	b.trigger.DatabaseName = dbName
	b.updateBinding()
	return b
}

// Container sets the CosmosDB container name.
func (b *CosmosFunctionBuilder) Container(containerName string) *CosmosFunctionBuilder {
	b.trigger.ContainerName = containerName
	b.updateBinding()
	return b
}

// BlobFunctionBuilder is a builder for creating Blob triggered functions.
type BlobFunctionBuilder struct {
	trigger *bindings.Blob
	rf      *RegisteredFunction
}

// Blob creates a new Blob triggered function.
func (app *App) Blob(name string, f interface{}) *BlobFunctionBuilder {
	trigger := &bindings.Blob{
		Name: "blob",
	}

	rf := app.RegisterFunction(f, trigger)

	return &BlobFunctionBuilder{
		trigger: trigger,
		rf:      rf,
	}
}

// Path sets the blob path.
func (b *BlobFunctionBuilder) Path(path string) *BlobFunctionBuilder {
	b.trigger.Path = path
	b.updateBinding()
	return b
}

// Connection sets the blob connection string setting.
func (b *BlobFunctionBuilder) Connection(connection string) *BlobFunctionBuilder {
	b.trigger.Connection = connection
	b.updateBinding()
	return b
}

// BlobOutput adds a blob output binding.
func (b *BlobFunctionBuilder) BlobOutput(name, path, connection string) *BlobFunctionBuilder {
	blobOutput := &bindings.BlobOutput{
		Name:       name,
		Path:       path,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, blobOutput.ToBinding())
	return b
}

func (b *BlobFunctionBuilder) updateBinding() {
	if len(b.rf.RawBindings) > 0 {
		newBinding := b.trigger.ToBinding()
		b.rf.RawBindings[0] = newBinding
	}
}

// Connection sets the CosmosDB connection string setting.
func (b *CosmosFunctionBuilder) Connection(connection string) *CosmosFunctionBuilder {
	b.trigger.Connection = connection
	b.updateBinding()
	return b
}

func (b *CosmosFunctionBuilder) updateBinding() {
	if len(b.rf.RawBindings) > 0 {
		newBinding := b.trigger.ToBinding()
		b.rf.RawBindings[0] = newBinding
	}
}

// EventGridFunctionBuilder is a builder for creating EventGrid triggered functions.
type EventGridFunctionBuilder struct {
	trigger *bindings.EventGridTrigger
	rf      *RegisteredFunction
}

// EventGrid creates a new EventGrid triggered function.
func (app *App) EventGrid(name string, f interface{}) *EventGridFunctionBuilder {
	trigger := &bindings.EventGridTrigger{
		Name: "event",
	}

	rf := app.RegisterFunction(f, trigger)

	return &EventGridFunctionBuilder{
		trigger: trigger,
		rf:      rf,
	}
}

// EventGridOutput adds an EventGrid output binding.
func (b *EventGridFunctionBuilder) EventGridOutput(name, topicEndpointUri, topicKeySetting string) *EventGridFunctionBuilder {
	output := &bindings.EventGridOutput{
		Name:             name,
		TopicEndpointUri: topicEndpointUri,
		TopicKeySetting:  topicKeySetting,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// RegisteredFunction holds metadata about a registered function.
type RegisteredFunction struct {
	Func        interface{}
	FuncName    string
	FuncId      string
	RawBindings []bindings.Binding
	Retry       *RetryOptions
	ScriptFile  string
}

// RegisterFunction registers a function with a trigger binding.
func (app *App) RegisterFunction(f interface{}, b bindings.Bind) *RegisteredFunction {
	triggerBinding := b.ToBinding()
	rawBindings := []bindings.Binding{triggerBinding}

	// If this is an HTTP Trigger, we implicitly add the HTTP Output binding
	if b.GetBindingType() == bindings.HttpBindingType {
		rawBindings = append(rawBindings, bindings.Binding{
			Name:      "$return",
			Type:      "http",
			Direction: "out",
		})
	}

	ptr := reflect.ValueOf(f).Pointer()
	fun := runtime.FuncForPC(ptr)
	file, _ := fun.FileLine(ptr)

	rf := &RegisteredFunction{
		Func:        f,
		FuncName:    GetFunctionName(f),
		ScriptFile:  file,
		RawBindings: rawBindings,
	}

	funcId, err := HashFunctionID(*rf)
	if err != nil {
		panic(err)
	}

	rf.FuncId = funcId
	app.RegisteredFunctions.Store(funcId, rf)
	return rf
}

// WithRetry adds a retry policy to a registered function.
func (rf *RegisteredFunction) WithRetry(retry *RetryOptions) *RegisteredFunction {
	rf.Retry = retry
	return rf
}

// GetFunctionName returns the simple name of the function, stripping the package path.
// The Azure Functions Host does not support dots in function names, so we need to
// extract the function name from the fully qualified Go name (pkg.Func).
func GetFunctionName(f interface{}) string {
	fullName := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	parts := strings.Split(fullName, ".")
	return parts[len(parts)-1]
}

// HashFunctionID generates a unique ID for the function.
func HashFunctionID(rf RegisteredFunction) (string, error) {
	// Create a unique string based on function name.
	var sb strings.Builder
	sb.WriteString(rf.FuncName)

	hash := sha256.New()
	if _, err := hash.Write([]byte(sb.String())); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
