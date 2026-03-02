# Event Grid Trigger Sample

An Azure Function that triggers on Azure Event Grid events. The event data is deserialized into a typed `EventGridEvent` struct.

## Prerequisites

- [Go 1.24+](https://go.dev/dl/)
- Custom [Azure Functions Core Tools](https://www.npmjs.com/package/@gaaguiar/azure-functions-core-tools) with Go worker support:
  ```bash
  npm i -g @gaaguiar/azure-functions-core-tools
  ```

## Setup

```bash
cd samples/eventGridTrigger
go mod init myapp
go mod edit -require github.com/azure/azure-functions-golang-worker@v0.0.0
go mod edit -replace github.com/azure/azure-functions-golang-worker=../..
go mod tidy
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

Send a test event to the Event Grid webhook endpoint. You will need the system key from the function host.

Get the system key:

```bash
curl http://localhost:7071/admin/host/systemkeys/eventgrid_extension
```

Send a test event:

```bash
curl -X POST "http://localhost:7071/runtime/webhooks/EventGrid?functionName=EventGridHandler&code=<your-system-key>" \
  -H "Content-Type: application/json" \
  -H "aeg-event-type: Notification" \
  -d '[{
    "id": "test-id-1",
    "topic": "/subscriptions/xxx/resourceGroups/xxx",
    "subject": "test-subject",
    "eventType": "Microsoft.Storage.BlobCreated",
    "eventTime": "2026-02-18T12:00:00Z",
    "dataVersion": "1",
    "metadataVersion": "1",
    "data": {"message": "hello"}
  }]'
```

Expected output in the function logs:

```
Event Grid trigger executed
Event ID: test-id-1
Event Type: Microsoft.Storage.BlobCreated
Subject: test-subject
Event Time: 2026-02-18T12:00:00Z
Event Data: map[message:hello]
```
