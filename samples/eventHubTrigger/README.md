# EventHub Trigger Sample

An Azure Function that triggers when events are sent to an Azure Event Hub. Uses the Azure Event Hubs SDK client for type-safe access.

## What this sample demonstrates

- A typed Event Hub handler `func(ctx context.Context, msg bindings.EventHubMessage) error` — the payload (body, partition key, properties, sequence number) is deserialized directly from the gRPC `InvocationRequest`.
- Structured logging via `slog.InfoContext` surfacing partition key and sequence number alongside the auto-attached invocation metadata, useful for filtering by partition in observability backends.

## Prerequisites

- [Go 1.24+](https://go.dev/dl/)
- Custom [Azure Functions Core Tools](https://www.npmjs.com/package/@gaaguiar/azure-functions-core-tools) with Go worker support:
  ```bash
  npm i -g @gaaguiar/azure-functions-core-tools
  ```
- An Azure Event Hub namespace and event hub

## Setup

```bash
cd samples/eventHubTrigger
go mod init myapp
go get github.com/azure/azure-functions-golang-worker
go mod tidy
```

Update `local.settings.json` with your Event Hub connection string:

```json
{
  "Values": {
    "EventHubConnection": "<your-eventhub-connection-string>"
  }
}
```

## Run

```bash
func start
```

`func start` automatically builds the Go project before launching. To skip the build step (e.g., if you've already built manually), use:

```bash
func start --no-build
```

## Test

Send an event to your `myeventhub` event hub. The function will trigger and log the Event Hub properties including partition information.
