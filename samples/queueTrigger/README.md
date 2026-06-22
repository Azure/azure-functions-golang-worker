# Queue Storage Trigger Sample

Demonstrates an Azure Storage Queue triggered function in Go.

## Prerequisites

- [Go 1.24+](https://go.dev/dl/)
- [Azure Functions Core Tools v4.12.0+](https://www.npmjs.com/package/azure-functions-core-tools)
- [Azurite](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite) (local storage emulator)

## Running Locally

1. Start Azurite (or use the docker-compose in `tests/emulators/`):

   ```bash
   docker compose -f tests/emulators/docker-compose.yml up azurite -d
   ```

2. Start the function app:

   ```bash
   cd samples/queueTrigger
   func start
   ```

3. Send a message to the queue using Azure Storage Explorer, `az` CLI, or the Azure SDK:

   ```bash
   az storage message put \
     --queue-name myqueue \
     --content "Hello from Storage Queue!" \
     --connection-string "UseDevelopmentStorage=true"
   ```

4. Observe the function trigger in the console output with message metadata.

## Configuration

| Option | Description |
|--------|-------------|
| `sdk.WithQueueName("myqueue")` | The name of the Azure Storage Queue to monitor |
| `sdk.WithConnection("AzureWebJobsStorage")` | App setting name containing the storage connection string |
