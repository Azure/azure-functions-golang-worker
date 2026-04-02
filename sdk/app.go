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
	registeredFunctions *sync.Map
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

// --- HTTP Trigger ---

// HttpFunctionBuilder is a builder for creating HTTP triggered functions.
type HttpFunctionBuilder struct {
	trigger *bindings.HttpTrigger
	rf      *RegisteredFunction
}

// HTTP creates a new HTTP triggered function.
func (app *App) HTTP(name string, f HTTPHandler) *HttpFunctionBuilder {
	trigger := &bindings.HttpTrigger{
		Name:      "req",
		Route:     name,
		AuthLevel: "anonymous",
		Methods:   []string{"GET", "POST"},
	}

	rf := app.registerFunction(f, trigger)

	return &HttpFunctionBuilder{
		trigger: trigger,
		rf:      rf,
	}
}

// Methods sets the allowed HTTP methods.
func (b *HttpFunctionBuilder) Methods(methods ...string) *HttpFunctionBuilder {
	b.trigger.Methods = methods
	b.updateBinding()
	return b
}

// Auth sets the authorization level.
func (b *HttpFunctionBuilder) Auth(level string) *HttpFunctionBuilder {
	b.trigger.AuthLevel = level
	b.updateBinding()
	return b
}

func (b *HttpFunctionBuilder) updateBinding() {
	if len(b.rf.RawBindings) > 0 {
		b.rf.RawBindings[0] = b.trigger.ToBinding()
	}
}

// --- Timer Trigger ---

// TimerFunctionBuilder provides a fluent API for configuring timer-triggered functions.
type TimerFunctionBuilder struct {
	trigger *bindings.TimerTrigger
	rf      *RegisteredFunction
}

// Timer creates a new timer-triggered function with the given name.
func (app *App) Timer(name string, f TimerHandler) *TimerFunctionBuilder {
	trigger := &bindings.TimerTrigger{
		Name: "timer",
	}

	rf := app.registerFunction(f, trigger)

	return &TimerFunctionBuilder{
		trigger: trigger,
		rf:      rf,
	}
}

// Schedule sets the NCrontab CRON expression for the timer trigger.
func (b *TimerFunctionBuilder) Schedule(schedule string) *TimerFunctionBuilder {
	b.trigger.Schedule = schedule
	b.updateBinding()
	return b
}

func (b *TimerFunctionBuilder) updateBinding() {
	if len(b.rf.RawBindings) > 0 {
		b.rf.RawBindings[0] = b.trigger.ToBinding()
	}
}

// --- CosmosDB Trigger ---

// CosmosFunctionBuilder is a builder for creating CosmosDB triggered functions.
type CosmosFunctionBuilder struct {
	trigger *bindings.CosmosDB
	rf      *RegisteredFunction
}

// CosmosDB creates a new CosmosDB triggered function.
func (app *App) CosmosDB(name string, f CosmosDBHandler) *CosmosFunctionBuilder {
	trigger := &bindings.CosmosDB{
		ArgName: "docs",
	}

	rf := app.registerFunction(f, trigger)

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

// Connection sets the CosmosDB connection string setting.
func (b *CosmosFunctionBuilder) Connection(connection string) *CosmosFunctionBuilder {
	b.trigger.Connection = connection
	b.updateBinding()
	return b
}

func (b *CosmosFunctionBuilder) updateBinding() {
	if len(b.rf.RawBindings) > 0 {
		b.rf.RawBindings[0] = b.trigger.ToBinding()
	}
}

// --- EventGrid Trigger ---

// EventGridFunctionBuilder is a builder for creating EventGrid triggered functions.
type EventGridFunctionBuilder struct {
	trigger *bindings.EventGridTrigger
	rf      *RegisteredFunction
}

// EventGrid creates a new EventGrid triggered function.
func (app *App) EventGrid(name string, f EventGridHandler) *EventGridFunctionBuilder {
	trigger := &bindings.EventGridTrigger{
		Name: "event",
	}

	rf := app.registerFunction(f, trigger)

	return &EventGridFunctionBuilder{
		trigger: trigger,
		rf:      rf,
	}
}

// --- EventHub Trigger ---

// EventHubFunctionBuilder is a builder for creating EventHub triggered functions.
type EventHubFunctionBuilder struct {
	trigger *bindings.EventHubTrigger
	rf      *RegisteredFunction
}

// EventHub creates a new EventHub triggered function.
func (app *App) EventHub(name string, f EventHubHandler) *EventHubFunctionBuilder {
	trigger := &bindings.EventHubTrigger{
		Name:          "message",
		ConsumerGroup: "$Default",
		Cardinality:   "one",
	}

	rf := app.registerFunction(f, trigger)

	return &EventHubFunctionBuilder{
		trigger: trigger,
		rf:      rf,
	}
}

// EventHubName sets the EventHub name.
func (b *EventHubFunctionBuilder) EventHubName(name string) *EventHubFunctionBuilder {
	b.trigger.EventHubName = name
	b.updateBinding()
	return b
}

// Connection sets the EventHub connection string setting name.
func (b *EventHubFunctionBuilder) Connection(connection string) *EventHubFunctionBuilder {
	b.trigger.Connection = connection
	b.updateBinding()
	return b
}

// ConsumerGroup sets the EventHub consumer group.
func (b *EventHubFunctionBuilder) ConsumerGroup(group string) *EventHubFunctionBuilder {
	b.trigger.ConsumerGroup = group
	b.updateBinding()
	return b
}

// Cardinality sets the EventHub trigger cardinality ("one" or "many").
func (b *EventHubFunctionBuilder) Cardinality(cardinality string) *EventHubFunctionBuilder {
	b.trigger.Cardinality = cardinality
	b.updateBinding()
	return b
}

func (b *EventHubFunctionBuilder) updateBinding() {
	if len(b.rf.RawBindings) > 0 {
		b.rf.RawBindings[0] = b.trigger.ToBinding()
	}
}

// --- Service Bus Queue Trigger ---

// ServiceBusQueueFunctionBuilder is a builder for creating Service Bus queue triggered functions.
type ServiceBusQueueFunctionBuilder struct {
	trigger *bindings.ServiceBusQueueTrigger
	rf      *RegisteredFunction
}

// ServiceBusQueue creates a new Service Bus queue triggered function.
func (app *App) ServiceBusQueue(name string, f ServiceBusHandler) *ServiceBusQueueFunctionBuilder {
	trigger := &bindings.ServiceBusQueueTrigger{
		Name:        "message",
		Cardinality: "one",
	}

	rf := app.registerFunction(f, trigger)

	return &ServiceBusQueueFunctionBuilder{
		trigger: trigger,
		rf:      rf,
	}
}

// QueueName sets the Service Bus queue name.
func (b *ServiceBusQueueFunctionBuilder) QueueName(queueName string) *ServiceBusQueueFunctionBuilder {
	b.trigger.QueueName = queueName
	b.updateBinding()
	return b
}

// Connection sets the Service Bus connection string setting name.
func (b *ServiceBusQueueFunctionBuilder) Connection(connection string) *ServiceBusQueueFunctionBuilder {
	b.trigger.Connection = connection
	b.updateBinding()
	return b
}

// IsSessionsEnabled sets whether sessions are enabled on the queue.
func (b *ServiceBusQueueFunctionBuilder) IsSessionsEnabled(enabled bool) *ServiceBusQueueFunctionBuilder {
	b.trigger.IsSessionsEnabled = enabled
	b.updateBinding()
	return b
}

// Cardinality sets the Service Bus trigger cardinality ("one" or "many").
func (b *ServiceBusQueueFunctionBuilder) Cardinality(cardinality string) *ServiceBusQueueFunctionBuilder {
	b.trigger.Cardinality = cardinality
	b.updateBinding()
	return b
}

func (b *ServiceBusQueueFunctionBuilder) updateBinding() {
	if len(b.rf.RawBindings) > 0 {
		b.rf.RawBindings[0] = b.trigger.ToBinding()
	}
}

// --- Service Bus Topic Trigger ---

// ServiceBusTopicFunctionBuilder is a builder for creating Service Bus topic triggered functions.
type ServiceBusTopicFunctionBuilder struct {
	trigger *bindings.ServiceBusTopicTrigger
	rf      *RegisteredFunction
}

// ServiceBusTopic creates a new Service Bus topic triggered function.
func (app *App) ServiceBusTopic(name string, f ServiceBusHandler) *ServiceBusTopicFunctionBuilder {
	trigger := &bindings.ServiceBusTopicTrigger{
		Name:        "message",
		Cardinality: "one",
	}

	rf := app.registerFunction(f, trigger)

	return &ServiceBusTopicFunctionBuilder{
		trigger: trigger,
		rf:      rf,
	}
}

// TopicName sets the Service Bus topic name.
func (b *ServiceBusTopicFunctionBuilder) TopicName(topicName string) *ServiceBusTopicFunctionBuilder {
	b.trigger.TopicName = topicName
	b.updateBinding()
	return b
}

// SubscriptionName sets the Service Bus subscription name.
func (b *ServiceBusTopicFunctionBuilder) SubscriptionName(subscriptionName string) *ServiceBusTopicFunctionBuilder {
	b.trigger.SubscriptionName = subscriptionName
	b.updateBinding()
	return b
}

// Connection sets the Service Bus connection string setting name.
func (b *ServiceBusTopicFunctionBuilder) Connection(connection string) *ServiceBusTopicFunctionBuilder {
	b.trigger.Connection = connection
	b.updateBinding()
	return b
}

// IsSessionsEnabled sets whether sessions are enabled on the subscription.
func (b *ServiceBusTopicFunctionBuilder) IsSessionsEnabled(enabled bool) *ServiceBusTopicFunctionBuilder {
	b.trigger.IsSessionsEnabled = enabled
	b.updateBinding()
	return b
}

// Cardinality sets the Service Bus trigger cardinality ("one" or "many").
func (b *ServiceBusTopicFunctionBuilder) Cardinality(cardinality string) *ServiceBusTopicFunctionBuilder {
	b.trigger.Cardinality = cardinality
	b.updateBinding()
	return b
}

func (b *ServiceBusTopicFunctionBuilder) updateBinding() {
	if len(b.rf.RawBindings) > 0 {
		b.rf.RawBindings[0] = b.trigger.ToBinding()
	}
}

// --- Blob Trigger ---

// BlobFunctionBuilder is a builder for creating blob triggered functions.
// The handler receives a trigger-specific client (e.g., *blob.Client) created
// by the registered ClientFactory. Import the triggers/blob package to enable:
//
//	import _ "github.com/azure/azure-functions-golang-worker/triggers/blob"
type BlobFunctionBuilder struct {
	trigger *bindings.Blob
	rf      *RegisteredFunction
}

// Blob creates a new blob-triggered function.
// The handler argument type depends on the registered blob trigger extension.
// Import the triggers/blob package to enable *blob.Client support:
//
//	import _ "github.com/azure/azure-functions-golang-worker/triggers/blob"
func (app *App) Blob(name string, f any) *BlobFunctionBuilder {
	trigger := &bindings.Blob{
		Name: "blob",
	}

	rf := app.registerFunction(f, trigger)

	// Look up the globally registered factory for blob triggers
	if factory, ok := GetClientFactory(string(bindings.BlobBindingType)); ok {
		rf.ClientFactory = factory
	}

	return &BlobFunctionBuilder{
		trigger: trigger,
		rf:      rf,
	}
}

// Path sets the blob path pattern (e.g., "container/{name}").
func (b *BlobFunctionBuilder) Path(path string) *BlobFunctionBuilder {
	b.trigger.Path = path
	b.updateBinding()
	return b
}

// Connection sets the blob connection string setting name.
func (b *BlobFunctionBuilder) Connection(connection string) *BlobFunctionBuilder {
	b.trigger.Connection = connection
	b.updateBinding()
	return b
}

func (b *BlobFunctionBuilder) updateBinding() {
	if len(b.rf.RawBindings) > 0 {
		b.rf.RawBindings[0] = b.trigger.ToBinding()
	}
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

// RegisterFunction registers a function with a trigger binding.
// This is exported for use by external trigger modules (e.g., triggers/blob).
func (app *App) RegisterFunction(f any, b bindings.Bind) *RegisteredFunction {
	return app.registerFunction(f, b)
}

func (app *App) registerFunction(f any, b bindings.Bind) *RegisteredFunction {
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
		TriggerType: string(b.GetBindingType()),
	}

	funcId, err := HashFunctionID(*rf)
	if err != nil {
		panic(err)
	}

	rf.FuncId = funcId
	app.registeredFunctions.Store(funcId, rf)
	return rf
}

// WithRetry adds a retry policy to a registered function.
func (rf *RegisteredFunction) WithRetry(retry *RetryOptions) *RegisteredFunction {
	rf.Retry = retry
	return rf
}

// GetFunctionName returns the simple name of the function, stripping the package path.
func GetFunctionName(f any) string {
	fullName := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	parts := strings.Split(fullName, ".")
	return parts[len(parts)-1]
}

// HashFunctionID generates a unique ID for the function.
func HashFunctionID(rf RegisteredFunction) (string, error) {
	var sb strings.Builder
	sb.WriteString(rf.FuncName)

	hash := sha256.New()
	if _, err := hash.Write([]byte(sb.String())); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
