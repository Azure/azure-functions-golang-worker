# HTTP to EventHub Output Sample

An Azure Function with an HTTP trigger that sends events to an Azure Event Hub using the EventHub output binding with SDK types.

## Prerequisites

- [Go 1.24+](https://go.dev/dl/)
- Custom [Azure Functions Core Tools](https://www.npmjs.com/package/@gaaguiar/azure-functions-core-tools) with Go worker support:
  ```bash
  npm i -g @gaaguiar/azure-functions-core-tools
  ```
- An Azure Event Hub namespace (or the [Event Hubs Emulator](https://learn.microsoft.com/en-us/azure/event-hubs/test-locally-with-event-hub-emulator))

## Setup

```bash
cd samples/httpEventHubOutput
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

## Test

Send an HTTP request to trigger the function:

```bash
curl http://localhost:7071/api/send
```

The function sends an event to the `myeventhub` event hub. If you have the `eventHubTrigger` sample running alongside, it will pick up the event.
