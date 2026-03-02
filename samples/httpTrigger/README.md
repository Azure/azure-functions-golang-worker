# HTTP Trigger Sample

An Azure Function with an HTTP trigger that responds to GET and POST requests.

## Prerequisites

- [Go 1.24+](https://go.dev/dl/)
- Custom [Azure Functions Core Tools](https://www.npmjs.com/package/@gaaguiar/azure-functions-core-tools) with Go worker support:
  ```bash
  npm i -g @gaaguiar/azure-functions-core-tools
  ```

## Setup

```bash
cd samples/httpTrigger
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

```bash
curl http://localhost:7071/api/hello
```

Expected response: `Hello from Go Worker!`
