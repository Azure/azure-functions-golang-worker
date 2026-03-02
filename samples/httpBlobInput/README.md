# HTTP Trigger with Blob Input Sample

An Azure Function with an HTTP trigger that reads a blob using an input binding. The blob is accessed via an injected Azure Blob SDK client.

## Prerequisites

- [Go 1.24+](https://go.dev/dl/)
- Custom [Azure Functions Core Tools](https://www.npmjs.com/package/@gaaguiar/azure-functions-core-tools) with Go worker support:
  ```bash
  npm i -g @gaaguiar/azure-functions-core-tools
  ```
- An Azure Storage account (or [Azurite](https://github.com/Azure/Azurite) for local emulation)

## Setup

```bash
cd samples/httpBlobInput
go mod init myapp
go get github.com/azure/azure-functions-golang-worker
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

Create a blob container named `test-container` with a blob named `test.txt` in your storage account.

## Run

```bash
func start
```

`func start` automatically builds the Go project before launching. To skip the build step (e.g., if you've already built manually), use:

```bash
func start --no-build
```

## Test

```bash
curl http://localhost:7071/api/hello
```

Expected response: `Read blob content via SDK: <contents of test.txt>`
