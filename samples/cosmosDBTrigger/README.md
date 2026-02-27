# CosmosDB Trigger Sample

An Azure Function that triggers when documents are created or updated in an Azure Cosmos DB container.

## Prerequisites

- [Go 1.24+](https://go.dev/dl/)
- [Azure Functions Core Tools](https://github.com/Azure/azure-functions-core-tools) with Go worker support
- An Azure Cosmos DB account

## Setup

```bash
cd samples/cosmosDBTrigger
go mod init myapp
go mod edit -require github.com/azure/azure-functions-golang-worker@v0.0.0
go mod edit -replace github.com/azure/azure-functions-golang-worker=../..
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
