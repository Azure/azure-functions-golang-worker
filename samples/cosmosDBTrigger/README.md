# CosmosDB Trigger Sample

An Azure Function that triggers when documents are created or updated in an Azure Cosmos DB container.

## What this sample demonstrates

- A typed CosmosDB-trigger handler `func(ctx context.Context, docs []bindings.CosmosDocument) error` — documents are deserialized directly from the gRPC `InvocationRequest` into the `CosmosDocument` slice, no SDK client needed (the **core trigger** model in [`TECHNICAL_SPEC.md`](../../TECHNICAL_SPEC.md) section 4).
- Per-document structured logging with `slog.InfoContext` so every emitted record carries `invocation_id` and `function_name` plus user-supplied document attributes.

## Prerequisites

- [Go 1.24+](https://go.dev/dl/)
- Custom [Azure Functions Core Tools](https://www.npmjs.com/package/@gaaguiar/azure-functions-core-tools) with Go worker support:
  ```bash
  npm i -g @gaaguiar/azure-functions-core-tools
  ```
- An Azure Cosmos DB account

## Setup

```bash
cd samples/cosmosDBTrigger
go mod init myapp
go get github.com/azure/azure-functions-golang-worker
go mod tidy
```

Update `local.settings.json` with your Cosmos DB connection string:

```json
{
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "CosmosDBConnection": "<your-cosmosdb-connection-string>"
  }
}
```

Ensure the database `ToDoList` and container `Items` exist in your Cosmos DB account.

## Run

```bash
func start
```

`func start` automatically builds the Go project before launching. To skip the build step (e.g., if you've already built manually), use:

```bash
func start --no-build
```

## Test

Create or update a document in the `Items` container of the `ToDoList` database. The function will trigger and log the document ID and data.
