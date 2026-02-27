# Blob Trigger Sample

An Azure Function that triggers when a blob is created or updated in Azure Storage. Uses the Azure Blob SDK client for type-safe blob access.

## Prerequisites

- [Go 1.24+](https://go.dev/dl/)
- [Azure Functions Core Tools](https://github.com/Azure/azure-functions-core-tools) with Go worker support
- An Azure Storage account (or [Azurite](https://github.com/Azure/Azurite) for local emulation)

## Setup

```bash
cd samples/blobTrigger
go mod init myapp
go mod edit -require github.com/azure/azure-functions-golang-worker@v0.0.0
go mod edit -replace github.com/azure/azure-functions-golang-worker=../..
go mod tidy
```

Update `local.settings.json` with your storage connection string:

```json
{
  "Values": {
    "AzureWebJobsStorage": "<your-storage-connection-string>"
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

Upload a file to the `test-container` container with the blob name `test.txt` in your storage account. The function will trigger and log the blob content.
