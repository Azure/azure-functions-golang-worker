# CosmosDB Trigger Sample — Auto-Create Lease Container

An Azure Function that triggers on CosmosDB change-feed events and **auto-provisions the lease container** on first run.

## What this sample demonstrates

- The same typed CosmosDB-trigger handler as [`../cosmosDBTrigger`](../cosmosDBTrigger) — `func(ctx context.Context, docs []bindings.CosmosDocument) error`.
- Using `sdk.WithCreateLeaseContainerIfNotExists(true)` so the CosmosDB extension creates the `leases` container automatically instead of failing when it does not exist.

## Prerequisites

- [Go 1.24+](https://go.dev/dl/)
- [Azure Functions Core Tools](https://www.npmjs.com/package/azure-functions-core-tools/v/4.12.0) 4.12.0 or later:
  ```bash
  npm i -g azure-functions-core-tools@4 --unsafe-perm true
  ```
- An Azure Cosmos DB account (or the Cosmos DB emulator)

## Setup

```bash
cd samples/cosmosDBTriggerAutoLease
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

Ensure the database `ToDoList` and the monitored container `Items` exist. The `leases` container **does not** need to exist beforehand — it will be created on first run.

## Run

```bash
func start
```

## Test

Create or update a document in the `Items` container of the `ToDoList` database. The first invocation will create the `leases` container if it does not already exist, then log the document ID and data.
