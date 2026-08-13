# Triggers and bindings

Understand the trigger models, binding configuration, and handler signatures
supported by the Go worker.

## Trigger models

A trigger starts a function invocation. The Azure Functions host listens to the
configured service and dispatches the invocation to the Go app. Trigger
configuration is registered through methods on `sdk.App` and reported to the
host during worker-driven indexing.

### Core triggers

Core triggers receive bounded invocation data from the host. The worker
converts the protocol payload into a typed Go value before calling the handler.
These triggers do not require an Azure SDK dependency in the compiled app.

HTTP is a special case. When the loopback HTTP listener is available, the host
proxies the request and response over HTTP to support streaming while gRPC
carries invocation correlation data. The worker falls back to buffered gRPC
HTTP payloads if proxying is unavailable or disabled.

### Extension triggers

Extension triggers are used when a resource can be large or the handler needs
streaming, random access, or write-back operations. The host sends resource
metadata instead of the full payload, and the worker injects an authenticated
Azure SDK client scoped to the triggering resource.

This model is more efficient for large resources because the host does not read,
serialize, and funnel the resource content through the gRPC channel. The worker
receives the metadata needed to create the client, and the handler streams or
reads the data directly from the backing Azure service. This reduces host and
worker memory pressure and avoids an extra copy through the invocation
protocol, although the handler still makes a service request through the
injected client.

Extension dependencies are isolated under `triggers/<name>`. Import the
extension package to register its client factory:

```go
import _ "github.com/azure/azure-functions-golang-worker/triggers/blob"
```

| Characteristic | Core trigger | Extension trigger |
| --- | --- | --- |
| Invocation data | Typed payload from the host | Resource metadata from the host |
| Handler argument | SDK binding type | Azure SDK client |
| External Azure SDK dependency | No | Yes, isolated in the extension module |
| Typical payload | Bounded messages, events, or change batches | Potentially large resources |
| Access pattern | Deserialize and process | Stream, seek, or write through the client |

## Supported triggers

| Trigger | Model | Registration method | Handler signature |
| --- | --- | --- | --- |
| HTTP | Core | `app.HTTP` | `func(http.ResponseWriter, *http.Request)` |
| Timer | Core | `app.Timer` | `func(context.Context, bindings.TimerInfo) error` |
| Azure Queue Storage | Core | `app.Queue` | `func(context.Context, bindings.QueueMessage) error` |
| Azure Cosmos DB | Core | `app.CosmosDB` | `func(context.Context, []bindings.CosmosDocument) error` |
| Azure Event Grid | Core | `app.EventGrid` | `func(context.Context, bindings.EventGridEvent) error` |
| Azure Event Hubs | Core | `app.EventHub` | `func(context.Context, bindings.EventHubMessage) error` |
| Azure Service Bus queue | Core | `app.ServiceBusQueue` | `func(context.Context, bindings.ServiceBusMessage) error` |
| Azure Service Bus topic | Core | `app.ServiceBusTopic` | `func(context.Context, bindings.ServiceBusMessage) error` |
| Azure SQL | Core | `app.SQL` | `func(context.Context, []bindings.SQLChange) error` |
| Azure Blob Storage | Extension | `app.Blob` | `func(context.Context, *blob.Client) error` |

See the [samples](../samples/index.md) for complete registration and handler
examples.

## Binding support

The current Go worker supports the trigger bindings listed above and the
implicit HTTP `$return` output binding, which is exposed through
`http.ResponseWriter`. Additional input bindings and non-HTTP output bindings
are not currently supported.

Registration options such as `sdk.WithQueueName`, `sdk.WithConnection`,
`sdk.WithConsumerGroup`, and `sdk.WithTable` configure the trigger binding
metadata sent to the host. Connection options name an application setting; they
do not contain the connection string itself.
