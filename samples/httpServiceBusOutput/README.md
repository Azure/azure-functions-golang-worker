# HTTP Trigger with Service Bus Output Bindings Sample

An Azure Function with an HTTP trigger that sends messages to both a Service Bus queue and topic via output bindings.

Use this sample together with the `serviceBusQueueTrigger` and `serviceBusTopicTrigger` samples to test end-to-end message flow: send an HTTP request here, and watch the trigger samples pick up the messages.

## Prerequisites

- [Go 1.24+](https://go.dev/dl/)
- Custom [Azure Functions Core Tools](https://www.npmjs.com/package/@gaaguiar/azure-functions-core-tools) with Go worker support:
  ```bash
  npm i -g @gaaguiar/azure-functions-core-tools
  ```
- An Azure Service Bus namespace with:
  - A queue named `input-queue`
  - A topic named `orders`

## Setup

```bash
cd samples/httpServiceBusOutput
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

Update the queue and topic names in `main.go` if yours differ from `input-queue` / `orders`.

## Run

```bash
func start
```

Then send a request:

```bash
curl http://localhost:7071/api/send
```

## End-to-End Testing

1. Start `serviceBusQueueTrigger` on port 7072: `func start --port 7072`
2. Start `serviceBusTopicTrigger` on port 7071: `func start --port 7071`
3. Start this sample on port 7073: `func start --port 7073`
4. Send an HTTP request: `curl http://localhost:7073/api/send`
5. Watch the trigger samples log the received messages
