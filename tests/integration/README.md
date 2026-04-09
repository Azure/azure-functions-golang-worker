# Integration Tests

Black-box integration tests for the Azure Functions Go Worker. Each test builds a sample app, starts it via `func.exe`, and verifies behavior through log output and SDK clients.

## Prerequisites

1. **Go 1.24+**
2. **Azure Functions Core Tools** (`func.exe`) — built or installed
3. **Docker** — for running emulators

### Start emulators

```bash
docker compose -f ../emulators/docker-compose.yml up -d
```

This starts:
- **Azurite** — Azure Storage emulator (ports 10000-10002)
- **Service Bus emulator** — (port 5672 on 127.0.0.1)
- **Event Hub emulator** — (port 5672 on 127.0.0.2)
- **Cosmos DB emulator** — (port 8081)
- **SQL Server** — backing store for SB/EH emulators (port 1433)

## Running

```bash
cd tests/integration

# Run all tests (requires func on PATH)
go test -v -timeout 300s ./...

# Run a specific test
go test -v -timeout 120s -run TestHttpTrigger ./...
```

To use a custom `func` binary (e.g. a local build), set `FUNC_EXE`:
```bash
export FUNC_EXE=/path/to/func               # Linux/macOS
$env:FUNC_EXE = "C:\path\to\func.exe"       # PowerShell
```

Tests will fail immediately if a required emulator is not running, with a message showing which emulator is missing and how to start it.

## Adding a new integration test

1. **Create a sample app** in `../../samples/<name>/` with a `main.go` and `host.json`.

2. **Create a test file** named `<name>_test.go` in this directory. Use `StartFuncHost` from `helpers_test.go`:

   ```go
   package integration

   import (
       "testing"
       "time"
   )

   var myEnv = map[string]string{
       "AzureWebJobsStorage":      "UseDevelopmentStorage=true",
       "FUNCTIONS_WORKER_RUNTIME": "golang",
       // Add any connection strings the trigger needs
   }

   func TestMyTriggerFires(t *testing.T) {
       requireAzurite(t)  // fail fast if emulator is down
       proc := StartFuncHost(t, "mySample", 7220, myEnv, 30*time.Second)

       // Use an Azure SDK client to send an event / upload data / etc.

       proc.AssertLogContains("expected log output", 30*time.Second)
       proc.AssertLogContains("Succeeded", 5*time.Second)
   }
   ```

3. **Pick a unique port** — check existing tests to avoid collisions (current range: 7201-7209).

4. **If the test needs a new Azure SDK**, add it to `go.mod`:
   ```bash
   cd tests/integration
   go get github.com/Azure/azure-sdk-for-go/sdk/...@latest
   go mod tidy
   ```
   This only affects the test module — the worker library's `go.mod` is not touched.

5. **If the test needs a new emulator**, add the service to `../emulators/docker-compose.yml`.

6. **Add a `require*` call** as the first line of each test function. This ensures the test fails immediately with a clear message if the emulator isn't running, rather than timing out waiting for `func.exe` to start. Available helpers:
   - `requireAzurite(t)` — all tests need this (backs `AzureWebJobsStorage`)
   - `requireCosmosDB(t)` — Cosmos DB trigger tests
   - `requireServiceBus(t)` — Service Bus queue/topic tests
   - `requireEventHub(t)` — Event Hub trigger tests

   To add a new one, use `requireEmulator(t, "name", "host:port")` in `helpers_test.go`.

### `FuncHostProcess` API

- `StartFuncHost(t, sampleName, port, env, initTimeout)` — builds the sample, starts `func.exe`, waits for initialization
- `proc.AssertLogContains(pattern, timeout)` — fails the test if the pattern doesn't appear within timeout
- `proc.WaitForLog(pattern, timeout) bool` — returns true/false without failing
- `proc.ReadLog() string` — returns the current log contents
- Cleanup (kill process, remove binary) is automatic via `t.Cleanup`

## Architecture

This directory is a **separate Go module** (`tests/integration/go.mod`) so that test-only SDK dependencies (`azcosmos`, `azeventhubs`, `azservicebus`) don't pollute the worker library's dependency tree.

Tests are fully black-box — they don't import any code from the worker module. Instead they:

1. `go build` a sample app from `../../samples/<name>/`
2. Start `func.exe` pointing at that sample directory
3. Wait for `"Worker process started and initialized"` in the log
4. Use Azure SDK clients to send events / upload blobs / insert documents
5. Assert expected log patterns appear within a timeout

The shared test harness is in `helpers_test.go` (`StartFuncHost`, `FuncHostProcess`).
