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

// EventHubOutput adds an EventHub output binding.
func (b *HttpFunctionBuilder) EventHubOutput(name, eventHubName, connection string) *HttpFunctionBuilder {
	output := &bindings.EventHubOutput{
		Name:         name,
		EventHubName: eventHubName,
		Connection:   connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// ServiceBusQueueOutput adds a Service Bus queue output binding.
func (b *HttpFunctionBuilder) ServiceBusQueueOutput(name, queueName, connection string) *HttpFunctionBuilder {
	output := &bindings.ServiceBusQueueOutput{
		Name:       name,
		QueueName:  queueName,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// ServiceBusTopicOutput adds a Service Bus topic output binding.
func (b *HttpFunctionBuilder) ServiceBusTopicOutput(name, topicName, connection string) *HttpFunctionBuilder {
	output := &bindings.ServiceBusTopicOutput{
		Name:      name,
		TopicName: topicName,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// EventGridOutput adds an EventGrid output binding.
func (b *HttpFunctionBuilder) EventGridOutput(name, topicEndpointUri, topicKeySetting string) *HttpFunctionBuilder {
	output := &bindings.EventGridOutput{
		Name:             name,
		TopicEndpointUri: topicEndpointUri,
		TopicKeySetting:  topicKeySetting,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
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

// EventHubOutput adds an EventHub output binding.
func (b *BlobFunctionBuilder) EventHubOutput(name, eventHubName, connection string) *BlobFunctionBuilder {
	output := &bindings.EventHubOutput{
		Name:         name,
		EventHubName: eventHubName,
		Connection:   connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// ServiceBusQueueOutput adds a Service Bus queue output binding.
func (b *BlobFunctionBuilder) ServiceBusQueueOutput(name, queueName, connection string) *BlobFunctionBuilder {
	output := &bindings.ServiceBusQueueOutput{
		Name:       name,
		QueueName:  queueName,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// ServiceBusTopicOutput adds a Service Bus topic output binding.
func (b *BlobFunctionBuilder) ServiceBusTopicOutput(name, topicName, connection string) *BlobFunctionBuilder {
	output := &bindings.ServiceBusTopicOutput{
		Name:      name,
		TopicName: topicName,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// BlobInput adds a blob input binding.
func (b *BlobFunctionBuilder) BlobInput(name, path, connection string) *BlobFunctionBuilder {
	blobInput := &bindings.BlobInput{
		Name:       name,
		Path:       path,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, blobInput.ToBinding())
	return b
}

// EventGridOutput adds an EventGrid output binding.
func (b *BlobFunctionBuilder) EventGridOutput(name, topicEndpointUri, topicKeySetting string) *BlobFunctionBuilder {
	output := &bindings.EventGridOutput{
		Name:             name,
		TopicEndpointUri: topicEndpointUri,
		TopicKeySetting:  topicKeySetting,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
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

// EventHubOutput adds an EventHub output binding.
func (b *CosmosFunctionBuilder) EventHubOutput(name, eventHubName, connection string) *CosmosFunctionBuilder {
	output := &bindings.EventHubOutput{
		Name:         name,
		EventHubName: eventHubName,
		Connection:   connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// ServiceBusQueueOutput adds a Service Bus queue output binding.
func (b *CosmosFunctionBuilder) ServiceBusQueueOutput(name, queueName, connection string) *CosmosFunctionBuilder {
	output := &bindings.ServiceBusQueueOutput{
		Name:       name,
		QueueName:  queueName,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// ServiceBusTopicOutput adds a Service Bus topic output binding.
func (b *CosmosFunctionBuilder) ServiceBusTopicOutput(name, topicName, connection string) *CosmosFunctionBuilder {
	output := &bindings.ServiceBusTopicOutput{
		Name:      name,
		TopicName: topicName,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// BlobInput adds a blob input binding.
func (b *CosmosFunctionBuilder) BlobInput(name, path, connection string) *CosmosFunctionBuilder {
	blobInput := &bindings.BlobInput{
		Name:       name,
		Path:       path,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, blobInput.ToBinding())
	return b
}

// BlobOutput adds a blob output binding.
func (b *CosmosFunctionBuilder) BlobOutput(name, path, connection string) *CosmosFunctionBuilder {
	blobOutput := &bindings.BlobOutput{
		Name:       name,
		Path:       path,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, blobOutput.ToBinding())
	return b
}

// EventGridOutput adds an EventGrid output binding.
func (b *CosmosFunctionBuilder) EventGridOutput(name, topicEndpointUri, topicKeySetting string) *CosmosFunctionBuilder {
	output := &bindings.EventGridOutput{
		Name:             name,
		TopicEndpointUri: topicEndpointUri,
		TopicKeySetting:  topicKeySetting,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
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

// EventHubOutput adds an EventHub output binding.
func (b *EventGridFunctionBuilder) EventHubOutput(name, eventHubName, connection string) *EventGridFunctionBuilder {
	output := &bindings.EventHubOutput{
		Name:         name,
		EventHubName: eventHubName,
		Connection:   connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// ServiceBusQueueOutput adds a Service Bus queue output binding.
func (b *EventGridFunctionBuilder) ServiceBusQueueOutput(name, queueName, connection string) *EventGridFunctionBuilder {
	output := &bindings.ServiceBusQueueOutput{
		Name:       name,
		QueueName:  queueName,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// ServiceBusTopicOutput adds a Service Bus topic output binding.
func (b *EventGridFunctionBuilder) ServiceBusTopicOutput(name, topicName, connection string) *EventGridFunctionBuilder {
	output := &bindings.ServiceBusTopicOutput{
		Name:      name,
		TopicName: topicName,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// BlobInput adds a blob input binding.
func (b *EventGridFunctionBuilder) BlobInput(name, path, connection string) *EventGridFunctionBuilder {
	blobInput := &bindings.BlobInput{
		Name:       name,
		Path:       path,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, blobInput.ToBinding())
	return b
}

// BlobOutput adds a blob output binding.
func (b *EventGridFunctionBuilder) BlobOutput(name, path, connection string) *EventGridFunctionBuilder {
	blobOutput := &bindings.BlobOutput{
		Name:       name,
		Path:       path,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, blobOutput.ToBinding())
	return b
}

// TimerFunctionBuilder provides a fluent API for configuring timer-triggered functions.
type TimerFunctionBuilder struct {
	trigger *bindings.TimerTrigger
	rf      *RegisteredFunction
}

// Timer creates a new timer-triggered function with the given name.
// Use the returned builder to configure the CRON schedule:
//
//	app.Timer("scheduledTask", handler).Schedule("0 */5 * * * *")
func (app *App) Timer(name string, f interface{}) *TimerFunctionBuilder {
	trigger := &bindings.TimerTrigger{
		Name: "timer",
	}

	rf := app.RegisterFunction(f, trigger)

	return &TimerFunctionBuilder{
		trigger: trigger,
		rf:      rf,
	}
}

// Schedule sets the NCrontab CRON expression for the timer trigger.
// Azure Functions uses 6-field expressions: {second} {minute} {hour} {day} {month} {day-of-week}.
func (b *TimerFunctionBuilder) Schedule(schedule string) *TimerFunctionBuilder {
	b.trigger.Schedule = schedule
	b.updateBinding()
	return b
}

func (b *TimerFunctionBuilder) updateBinding() {
	if len(b.rf.RawBindings) > 0 {
		newBinding := b.trigger.ToBinding()
		b.rf.RawBindings[0] = newBinding
	}
}

// ServiceBusQueueOutput adds a Service Bus queue output binding.
func (b *TimerFunctionBuilder) ServiceBusQueueOutput(name, queueName, connection string) *TimerFunctionBuilder {
	output := &bindings.ServiceBusQueueOutput{
		Name:       name,
		QueueName:  queueName,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// ServiceBusTopicOutput adds a Service Bus topic output binding.
func (b *TimerFunctionBuilder) ServiceBusTopicOutput(name, topicName, connection string) *TimerFunctionBuilder {
	output := &bindings.ServiceBusTopicOutput{
		Name:      name,
		TopicName: topicName,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// BlobInput adds a blob input binding.
func (b *TimerFunctionBuilder) BlobInput(name, path, connection string) *TimerFunctionBuilder {
	blobInput := &bindings.BlobInput{
		Name:       name,
		Path:       path,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, blobInput.ToBinding())
	return b
}

// BlobOutput adds a blob output binding.
func (b *TimerFunctionBuilder) BlobOutput(name, path, connection string) *TimerFunctionBuilder {
	blobOutput := &bindings.BlobOutput{
		Name:       name,
		Path:       path,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, blobOutput.ToBinding())
	return b
}

// EventHubOutput adds an EventHub output binding.
func (b *TimerFunctionBuilder) EventHubOutput(name, eventHubName, connection string) *TimerFunctionBuilder {
	output := &bindings.EventHubOutput{
		Name:         name,
		EventHubName: eventHubName,
		Connection:   connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// EventGridOutput adds an EventGrid output binding.
func (b *TimerFunctionBuilder) EventGridOutput(name, topicEndpointUri, topicKeySetting string) *TimerFunctionBuilder {
	output := &bindings.EventGridOutput{
		Name:             name,
		TopicEndpointUri: topicEndpointUri,
		TopicKeySetting:  topicKeySetting,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// EventHubFunctionBuilder is a builder for creating EventHub triggered functions.
type EventHubFunctionBuilder struct {
	trigger *bindings.EventHubTrigger
	rf      *RegisteredFunction
}

// EventHub creates a new EventHub triggered function.
func (app *App) EventHub(name string, f interface{}) *EventHubFunctionBuilder {
	trigger := &bindings.EventHubTrigger{
		Name:          "message",
		ConsumerGroup: "$Default",
		Cardinality:   "one",
	}

	rf := app.RegisterFunction(f, trigger)

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

// EventHubOutput adds an EventHub output binding.
func (b *EventHubFunctionBuilder) EventHubOutput(name, eventHubName, connection string) *EventHubFunctionBuilder {
	output := &bindings.EventHubOutput{
		Name:         name,
		EventHubName: eventHubName,
		Connection:   connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// ServiceBusQueueOutput adds a Service Bus queue output binding.
func (b *EventHubFunctionBuilder) ServiceBusQueueOutput(name, queueName, connection string) *EventHubFunctionBuilder {
	output := &bindings.ServiceBusQueueOutput{
		Name:       name,
		QueueName:  queueName,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// ServiceBusTopicOutput adds a Service Bus topic output binding.
func (b *EventHubFunctionBuilder) ServiceBusTopicOutput(name, topicName, connection string) *EventHubFunctionBuilder {
	output := &bindings.ServiceBusTopicOutput{
		Name:      name,
		TopicName: topicName,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// BlobInput adds a blob input binding.
func (b *EventHubFunctionBuilder) BlobInput(name, path, connection string) *EventHubFunctionBuilder {
	blobInput := &bindings.BlobInput{
		Name:       name,
		Path:       path,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, blobInput.ToBinding())
	return b
}

// BlobOutput adds a blob output binding.
func (b *EventHubFunctionBuilder) BlobOutput(name, path, connection string) *EventHubFunctionBuilder {
	blobOutput := &bindings.BlobOutput{
		Name:       name,
		Path:       path,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, blobOutput.ToBinding())
	return b
}

// EventGridOutput adds an EventGrid output binding.
func (b *EventHubFunctionBuilder) EventGridOutput(name, topicEndpointUri, topicKeySetting string) *EventHubFunctionBuilder {
	output := &bindings.EventGridOutput{
		Name:             name,
		TopicEndpointUri: topicEndpointUri,
		TopicKeySetting:  topicKeySetting,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

func (b *EventHubFunctionBuilder) updateBinding() {
	if len(b.rf.RawBindings) > 0 {
		newBinding := b.trigger.ToBinding()
		b.rf.RawBindings[0] = newBinding
	}
}

// ServiceBusQueueFunctionBuilder is a builder for creating Service Bus queue triggered functions.
type ServiceBusQueueFunctionBuilder struct {
	trigger *bindings.ServiceBusQueueTrigger
	rf      *RegisteredFunction
}

// ServiceBusQueue creates a new Service Bus queue triggered function.
func (app *App) ServiceBusQueue(name string, f interface{}) *ServiceBusQueueFunctionBuilder {
	trigger := &bindings.ServiceBusQueueTrigger{
		Name:        "message",
		Cardinality: "one",
	}

	rf := app.RegisterFunction(f, trigger)

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

// ServiceBusQueueOutput adds a Service Bus queue output binding.
func (b *ServiceBusQueueFunctionBuilder) ServiceBusQueueOutput(name, queueName, connection string) *ServiceBusQueueFunctionBuilder {
	output := &bindings.ServiceBusQueueOutput{
		Name:       name,
		QueueName:  queueName,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// ServiceBusTopicOutput adds a Service Bus topic output binding.
func (b *ServiceBusQueueFunctionBuilder) ServiceBusTopicOutput(name, topicName, connection string) *ServiceBusQueueFunctionBuilder {
	output := &bindings.ServiceBusTopicOutput{
		Name:      name,
		TopicName: topicName,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// EventHubOutput adds an EventHub output binding.
func (b *ServiceBusQueueFunctionBuilder) EventHubOutput(name, eventHubName, connection string) *ServiceBusQueueFunctionBuilder {
	output := &bindings.EventHubOutput{
		Name:         name,
		EventHubName: eventHubName,
		Connection:   connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// BlobInput adds a blob input binding.
func (b *ServiceBusQueueFunctionBuilder) BlobInput(name, path, connection string) *ServiceBusQueueFunctionBuilder {
	blobInput := &bindings.BlobInput{
		Name:       name,
		Path:       path,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, blobInput.ToBinding())
	return b
}

// BlobOutput adds a blob output binding.
func (b *ServiceBusQueueFunctionBuilder) BlobOutput(name, path, connection string) *ServiceBusQueueFunctionBuilder {
	blobOutput := &bindings.BlobOutput{
		Name:       name,
		Path:       path,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, blobOutput.ToBinding())
	return b
}

// EventGridOutput adds an EventGrid output binding.
func (b *ServiceBusQueueFunctionBuilder) EventGridOutput(name, topicEndpointUri, topicKeySetting string) *ServiceBusQueueFunctionBuilder {
	output := &bindings.EventGridOutput{
		Name:             name,
		TopicEndpointUri: topicEndpointUri,
		TopicKeySetting:  topicKeySetting,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

func (b *ServiceBusQueueFunctionBuilder) updateBinding() {
	if len(b.rf.RawBindings) > 0 {
		newBinding := b.trigger.ToBinding()
		b.rf.RawBindings[0] = newBinding
	}
}

// ServiceBusTopicFunctionBuilder is a builder for creating Service Bus topic triggered functions.
type ServiceBusTopicFunctionBuilder struct {
	trigger *bindings.ServiceBusTopicTrigger
	rf      *RegisteredFunction
}

// ServiceBusTopic creates a new Service Bus topic triggered function.
func (app *App) ServiceBusTopic(name string, f interface{}) *ServiceBusTopicFunctionBuilder {
	trigger := &bindings.ServiceBusTopicTrigger{
		Name:        "message",
		Cardinality: "one",
	}

	rf := app.RegisterFunction(f, trigger)

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

// ServiceBusQueueOutput adds a Service Bus queue output binding.
func (b *ServiceBusTopicFunctionBuilder) ServiceBusQueueOutput(name, queueName, connection string) *ServiceBusTopicFunctionBuilder {
	output := &bindings.ServiceBusQueueOutput{
		Name:       name,
		QueueName:  queueName,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// ServiceBusTopicOutput adds a Service Bus topic output binding.
func (b *ServiceBusTopicFunctionBuilder) ServiceBusTopicOutput(name, topicName, connection string) *ServiceBusTopicFunctionBuilder {
	output := &bindings.ServiceBusTopicOutput{
		Name:      name,
		TopicName: topicName,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// EventHubOutput adds an EventHub output binding.
func (b *ServiceBusTopicFunctionBuilder) EventHubOutput(name, eventHubName, connection string) *ServiceBusTopicFunctionBuilder {
	output := &bindings.EventHubOutput{
		Name:         name,
		EventHubName: eventHubName,
		Connection:   connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

// BlobInput adds a blob input binding.
func (b *ServiceBusTopicFunctionBuilder) BlobInput(name, path, connection string) *ServiceBusTopicFunctionBuilder {
	blobInput := &bindings.BlobInput{
		Name:       name,
		Path:       path,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, blobInput.ToBinding())
	return b
}

// BlobOutput adds a blob output binding.
func (b *ServiceBusTopicFunctionBuilder) BlobOutput(name, path, connection string) *ServiceBusTopicFunctionBuilder {
	blobOutput := &bindings.BlobOutput{
		Name:       name,
		Path:       path,
		Connection: connection,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, blobOutput.ToBinding())
	return b
}

// EventGridOutput adds an EventGrid output binding.
func (b *ServiceBusTopicFunctionBuilder) EventGridOutput(name, topicEndpointUri, topicKeySetting string) *ServiceBusTopicFunctionBuilder {
	output := &bindings.EventGridOutput{
		Name:             name,
		TopicEndpointUri: topicEndpointUri,
		TopicKeySetting:  topicKeySetting,
	}
	b.rf.RawBindings = append(b.rf.RawBindings, output.ToBinding())
	return b
}

func (b *ServiceBusTopicFunctionBuilder) updateBinding() {
	if len(b.rf.RawBindings) > 0 {
		newBinding := b.trigger.ToBinding()
		b.rf.RawBindings[0] = newBinding
	}
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
