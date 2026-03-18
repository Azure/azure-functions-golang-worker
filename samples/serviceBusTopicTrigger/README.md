# Service Bus Topic Trigger Sample

An Azure Function that triggers when messages arrive on an Azure Service Bus topic subscription.

## Prerequisites

- [Go 1.24+](https://go.dev/dl/)
- Custom [Azure Functions Core Tools](https://www.npmjs.com/package/@gaaguiar/azure-functions-core-tools) with Go worker support:
  ```bash
  npm i -g @gaaguiar/azure-functions-core-tools
  ```
- An Azure Service Bus namespace with a topic named `orders` and a subscription named `processor`

## Setup

```bash
cd samples/serviceBusTopicTrigger
go mod init myapp
go get github.com/azure/azure-functions-golang-worker
go mod tidy
```

Update `local.settings.json` with your Service Bus connection string:

```json
{
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "ServiceBusConnection": "<your-servicebus-connection-string>"
  }
}
```

Update the topic and subscription names in `main.go` if yours differ from `orders` / `processor`.

## Run

```bash
func start
```

## End-to-End Testing

Use together with the `httpServiceBusOutput` sample to test the full message flow:

1. Start this sample on one port (e.g. `func start --port 7071`)
2. Start `httpServiceBusOutput` on another port (e.g. `func start --port 7073`)
3. Send an HTTP request: `curl http://localhost:7073/api/send`
4. Watch this sample log the received message
