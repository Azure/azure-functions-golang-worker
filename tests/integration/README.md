# Integration Tests

Black-box integration tests for the Azure Functions Go Worker. The suite verifies
that Azure Functions Core Tools can start the Functions host, establish the
worker channel, index Go functions, invoke them, and return or process the
expected output.

The tests use local Docker emulators only. They do not require Azure credentials
or deployed Azure resources.

```text
Azure DevOps Pipeline / Local Developer
                  |
                  | go run ./cmd/integrationtest
                  v
+--------------------------------------------------+
| Integration Test Runner                          |
|                                                  |
|  1. Validate Core Tools and Docker Compose       |
|  2. Start/reuse emulators and wait for readiness |
|  3. Run selected Go integration tests            |
|  4. Collect diagnostics and clean up              |
+----------------------+---------------------------+
                       |
                       | go test -run <scenario>
                       v
+--------------------------------------------------+
| Test Scenario                                    |
|                                                  |
|  - Prepare test data                             |
|  - Start the sample through testhost             |
|  - Send request/event                            |
|  - Assert response, output, or logs              |
+----------------------+---------------------------+
                       |
                       | testhost.Start(...)
                       v
+--------------------------------------------------+
| Test Host                                        |
|                                                  |
|  - Select dynamic port                           |
|  - Start Azure Functions Core Tools              |
|  - Wait for worker and host readiness            |
|  - Capture logs                                  |
|  - Stop the complete process tree                |
+----------------------+---------------------------+
                       |
                       v
          +---------------------------+
          | Azure Functions Host      |
          |        Core Tools         |
          +-------------+-------------+
                        |
                        | gRPC worker channel
                        v
          +---------------------------+
          | Go Worker                 |
          |                           |
          |  - Report/load functions  |
          |  - Dispatch invocation    |
          |  - Run registered handler |
          |  - Return result          |
          +---------------------------+

Diagnostics from the runner, emulator, host, and worker
are written to the artifacts directory.
```

## Prerequisites

1. **Go 1.25+**
2. **Azure Functions Core Tools 4.12.0 or later**
3. **Docker with Docker Compose**

Core Tools must be available as `func` on `PATH`. To use another executable,
set `FUNC_EXE`:

```bash
export FUNC_EXE=/path/to/func
```

```powershell
$env:FUNC_EXE = "C:\path\to\func.exe"
```

The runner validates the Core Tools version and Docker Compose before starting
the suite.

## Current coverage — HTTP worker lifecycle and invocation path

The current integration test verifies the worker's core HTTP path end to end:
Core Tools starts the native Go worker, the worker establishes its host
channel, reports and loads the registered HTTP function, dispatches
invocations to a standard `net/http` handler, and returns the handler's status
and body to the client. `TestHttpTriggerGet` exercises that path with both GET
and POST requests.

Run it from the integration-test module:

```bash
cd tests/integration
go run ./cmd/integrationtest
```

The runner executes these steps in order:

1. Validates Core Tools 4.12.0 or later and Docker Compose.
2. Creates the `artifacts` directory.
3. Starts Azurite if no container exists or if an existing container is stopped;
   reuses it if it is already running.
4. Waits for Azurite blob (`127.0.0.1:10000`) and queue (`127.0.0.1:10001`)
   service readiness.
5. Runs `TestHttpTriggerGet` via `go test`.
6. Captures Azurite container logs and the Go test log as artifacts.
7. Removes Azurite only if no Azurite container existed before the run. A
   pre-existing stopped container that was started by the runner is left in place
   afterward.

## Azure DevOps integration (planned)

Azure DevOps pipeline wiring for this runner is planned but not yet implemented.
The intended design is a Linux host job that runs:

```bash
cd tests/integration
go run ./cmd/integrationtest
```

The pipeline would provision Go, the pinned Core Tools release, and Docker before
invoking the runner. Pipeline YAML would own agent and tool provisioning only;
emulator lifecycle, readiness, test selection, diagnostics, and cleanup would
remain implemented by the repository runner.

The integration-test job is intended to become a required pre-merge PR check once
pipeline wiring is complete.

## Success criteria

A test passes only when:

1. Its required emulators report ready.
2. Core Tools starts the Functions host.
3. The host establishes the Go worker channel.
4. The expected function is indexed.
5. The scenario-specific invocation or trigger completes.
6. The test observes the expected response, metadata, or log output.
7. The host and emulator processes remain alive until the scenario completes.

Timing is informational. The suite does not enforce host-startup or invocation
performance thresholds.

## Artifacts

Each run writes diagnostics beneath `tests/integration/artifacts`. Existing
files for the same test run are replaced.

```text
artifacts/
├── azurite.log
├── go-test.log
└── TestHttpTriggerGet/
    └── httpTrigger-host.log
```

- `azurite.log` contains complete Azurite container output.
- `go-test.log` contains complete Go test output.
- `TestHttpTriggerGet/httpTrigger-host.log` contains complete Functions host and
  worker output for the HTTP trigger test.

On failure, the runner captures container logs before cleanup. A cleanup error is
reported without replacing the original test failure.

## Architecture

This directory is a separate Go module so test-only Azure SDK dependencies do
not affect the worker library's dependency tree.

```text
cmd/integrationtest
        |
        v
internal/testrunner
  - validates tools
  - manages Azurite lifecycle (start/reuse/cleanup)
  - waits for emulator readiness
  - runs Go tests
  - captures artifacts
        |
        v
internal/testhost
  - allocates a dynamic port
  - starts Core Tools
  - configures the native Go worker
  - waits for worker and host readiness
  - monitors unexpected exits
  - captures the complete host log
  - terminates the process tree
        |
        v
trigger-specific test
  - provisions test data
  - sends the stimulus
  - asserts functional behavior
```

### Ownership boundaries

- The **runner** owns shared tools, Azurite lifecycle, readiness, test
  selection, artifacts, and cleanup.
- `TestHost` owns one Functions host process and its Go worker children.
- A **test scenario** owns only resource setup, stimulus, and behavioral
  assertions.

Tests do not start Docker containers, choose fixed host ports, install tools, or
implement process cleanup.

## Emulator endpoints

| Emulator | Host endpoint | Readiness |
|---|---|---|
| Azurite Blob | `127.0.0.1:10000` | Blob service request |
| Azurite Queue | `127.0.0.1:10001` | Queue service request |
| Azurite Table | `127.0.0.1:10002` | Not probed by the current milestone |

Emulator images are pinned in `tests/emulators/docker-compose.yml`. Update image
versions deliberately and validate the affected tests after each update.

## Future work

The following are planned but not implemented in this milestone:

- **Additional trigger scenarios**: Timer, Blob Storage, Queue Storage, Event
  Grid, Event Hubs, Cosmos DB, Service Bus, and SQL trigger tests.
- **Additional emulator profiles**: Event Hubs, Cosmos DB, Service Bus, and SQL
  Server containers with their own readiness checks and lifecycle management.
- **JUnit output**: `go-test.xml` for Azure DevOps test result publishing.
- **Run metadata**: `run.json` recording tool versions, emulator image digests,
  timestamps, and source revision.
- **Structured profile diagnostics**: Per-profile log files under `artifacts/profiles/`.
- **Azure DevOps pipeline wiring**: Pipeline YAML, required PR check, and
  artifact publishing.
