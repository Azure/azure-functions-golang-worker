---
name: add-new-binding
description: "Guide for adding a new trigger or binding type to the Azure Functions Go Worker SDK. Use this when asked to add support for a new Azure Functions trigger type (e.g., Queue, ServiceBus, SignalR) or binding type."
---

# Adding a New Trigger/Binding Type

Every trigger type follows the same 3-layer pattern across 3 files, plus tests and a sample.

## Layer 1: `sdk/bindings/<type>.go` — Define binding types

Create a new file. Follow the exact structure of `timer.go` or `http.go`.

1. **Binding type constant** — a typed constant using `BindingType`:
   ```go
   const QueueTriggerBindingType BindingType = "queueTrigger"
   ```

2. **Wire format struct** — JSON-tagged struct embedded in `Binding`. These fields get flattened into the host JSON:
   ```go
   type QueueBinding struct {
       QueueName  string `json:"queueName"`
       Connection string `json:"connection"`
   }
   ```

3. **Runtime payload structs** — what the host sends when invoking. JSON-tagged, used for deserialization:
   ```go
   type QueueMessage struct {
       Body         json.RawMessage `json:"azfuncdata"`
       DequeueCount int             `json:"dequeueCount"`
       // ...
   }
   ```

   **Important**: For messaging triggers (Service Bus, EventHub, Queue, etc.), the message body is delivered as InputData, NOT in TriggerMetadata. Use the special `json:"azfuncdata"` tag on the Body field so the converter populates it from InputData. Other metadata fields (MessageId, DeliveryCount, etc.) come from TriggerMetadata.

   **TriggerMetadata key casing**: The Azure Functions host sends TriggerMetadata keys in PascalCase (e.g. `MessageId`, `DeliveryCount`, `SequenceNumber`). Use camelCase json tags in Go structs (e.g. `json:"messageId"`); the worker's converter does case-insensitive fallback matching automatically.

4. **User-facing trigger config** — NO JSON tags. This is what developers interact with:
   ```go
   type QueueTrigger struct {
       Name       string
       QueueName  string
       Connection string
   }
   ```

5. **Bind interface methods** — satisfy the `Bind` interface (Go interfaces are implicit):
   ```go
   func (q *QueueTrigger) GetBindingType() BindingType { return QueueTriggerBindingType }

   func (q *QueueTrigger) ToBinding() Binding {
       return Binding{
           Name:         q.Name,
           Type:         string(q.GetBindingType()),
           Direction:    "in",
           QueueBinding: &QueueBinding{QueueName: q.QueueName, Connection: q.Connection},
       }
   }
   ```

## Layer 2: `sdk/bindings/common.go` — Register in the union struct

Two changes:

1. **Add embedded pointer** to the `Binding` struct:
   ```go
   type Binding struct {
       Name      string `json:"name"`
       Type      string `json:"type"`
       Direction string `json:"direction"`

       *CosmosDBBinding
       *HTTPBinding
       *BlobBinding
       *EventGridBinding
       *QueueBinding          // <-- ADD THIS
   }
   ```

2. **Add nil check** in `MarshalJSON()`:
   ```go
   } else if b.QueueBinding != nil {
       sub = b.QueueBinding
   }
   ```

## Layer 3: `sdk/app.go` — Add the fluent builder API

1. **Builder struct**:
   ```go
   type QueueFunctionBuilder struct {
       trigger *bindings.QueueTrigger
       rf      *RegisteredFunction
   }
   ```

2. **Factory method on App**:
   ```go
   func (app *App) Queue(name string, f interface{}) *QueueFunctionBuilder {
       trigger := &bindings.QueueTrigger{
           Name: "message",
       }
       rf := app.RegisterFunction(f, trigger)
       return &QueueFunctionBuilder{trigger: trigger, rf: rf}
   }
   ```

3. **Configuration methods** — return the builder for chaining:
   ```go
   func (b *QueueFunctionBuilder) QueueName(queueName string) *QueueFunctionBuilder {
       b.trigger.QueueName = queueName
       b.updateBinding()
       return b
   }

   func (b *QueueFunctionBuilder) Connection(connection string) *QueueFunctionBuilder {
       b.trigger.Connection = connection
       b.updateBinding()
       return b
   }
   ```

4. **updateBinding method** — syncs trigger config back to the registered binding:
   ```go
   func (b *QueueFunctionBuilder) updateBinding() {
       if len(b.rf.RawBindings) > 0 {
           newBinding := b.trigger.ToBinding()
           b.rf.RawBindings[0] = newBinding
       }
   }
   ```

5. **Add output/input binding methods to ALL existing builders** — Every new output binding type must be composable with every trigger. Add the output method to every existing builder (HTTP, Cosmos, Blob, EventGrid, Timer, EventHub, ServiceBus, etc.). Similarly, if adding a new input binding, add it to all builders. Check the existing methods on each builder before adding to avoid duplicates. This is easy to miss and creates tech debt if skipped.

## Layer 4: Tests

Create `sdk/bindings/<type>_test.go` with these test cases:

- `TestGetBindingType` — returns correct constant
- `TestToBinding` — produces correct Binding struct (Name, Type, Direction, embedded pointer)
- `TestToBindingJSON` — JSON serialization flattens fields correctly
- `TestPayloadDeserialization` — runtime payload struct deserializes from JSON
- Table-driven tests for edge cases (e.g., boolean flags, missing fields)

## Layer 5: Sample

Create `samples/<type>Trigger/` with:
- `main.go` — handler function + registration using the fluent API
- `host.json` — standard Azure Functions host config
- `local.settings.json` — `FUNCTIONS_WORKER_RUNTIME: "golang"` + any connection strings
- `README.md` — prerequisites, setup steps (go mod init, go get, go mod tidy), run command (func start)

Do NOT include `go.mod`, `go.sum`, or compiled binaries in the sample.

## Layer 6: Deferred Binding Extension (optional)

If users should receive Azure SDK client types instead of raw JSON payloads, create `sdk/extensions/<type>/<type>.go`.

The extension registers converters in `init()` so it activates via blank import (`_ "...extensions/<type>"`):

```go
package eventhub

import (
    "github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
    "github.com/azure/azure-functions-golang-worker/sdk"
)

func init() {
    sdk.RegisterConverter((*azeventhubs.ConsumerClient)(nil), convertToConsumerClient)
    sdk.RegisterConverter((*azeventhubs.ProducerClient)(nil), convertToProducerClient)
}
```

Each converter function:
- Reads the connection string setting name from `config["connection"]`, resolves it via `os.Getenv`
- Reads binding-specific config (e.g., `config["eventHubName"]`, `config["path"]`)
- Constructs and returns the Azure SDK client as a `reflect.Value`

The worker invokes converters automatically:
- **Input bindings** (`Direction: "in"`): Called by `FromProto` via `convertToTypeValue` → `sdk.GetConverter`
- **Output bindings** (`Direction: "out"`): Called by `handleInvocationRequest` step 3b, which iterates output bindings mapped to function arguments and creates SDK clients for any with a registered converter

### Output bindings as function arguments

Some services use different SDK types for reading vs writing (e.g., EventHub `ConsumerClient` vs `ProducerClient`). Output bindings with registered converters are injected as function arguments alongside inputs. Add `.XxxOutput()` methods to all relevant builders (Http, Blob, Cosmos, EventGrid, etc.) so output bindings can compose with any trigger type.

## Key Concepts

- **Bind interface**: `GetBindingType()` + `ToBinding()`. Satisfied implicitly in Go.
- **Binding union struct**: Only one embedded pointer is non-nil at a time. `MarshalJSON()` flattens it.
- **Fluent builders**: Each setter returns `*Builder` for chaining. Each setter calls `updateBinding()` because `RegisterFunction` captures a snapshot at registration time.
- **Struct categories**: Wire format (JSON tags) vs user-facing config (no JSON tags) vs runtime payload (JSON tags). Keep them separate.
- **Deferred bindings**: Users receive Azure SDK clients directly. The worker constructs them at invocation time using binding config + connection strings from environment variables. Activated via blank import of `sdk/extensions/<type>/`.
- **Converter registry**: `sdk.RegisterConverter` maps a `reflect.Type` to a factory function. Works for both input and output bindings. Output binding converters are called by `handleInvocationRequest` step 3b.
