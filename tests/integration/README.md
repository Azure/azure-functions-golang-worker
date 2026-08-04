# Integration Tests

Black-box integration tests for the Azure Functions Go Worker. The suite verifies
that Azure Functions Core Tools can start the Functions host, establish the
worker channel, index Go functions, invoke them, and return or process the
expected output.

The tests use local Docker emulators only. They do not require Azure credentials
or deployed Azure resources.

## Prerequisites

1. **Go 1.25+**
2. **Azure Functions Core Tools 4.12.0**
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

## Running the suite

Run the complete suite from the integration-test module:

```bash
cd tests/integration
go run ./cmd/integrationtest
```

The runner:

1. Validates required tools.
2. Creates the artifact directory.
3. Starts each required emulator profile.
4. Waits for service-level readiness.
5. Runs the tests assigned to that profile.
6. Captures Go test, Functions host, and container logs.
7. Stops only the containers it started.

The suite runs emulator profiles sequentially to reduce resource usage and keep
failures attributable to one subsystem:

```text
Azurite
  ├── HTTP
  ├── Timer
  ├── Blob Storage
  ├── Queue Storage
  └── Event Grid registration

Event Hubs + Azurite
  └── Event Hub trigger

Cosmos DB + Azurite
  └── Cosmos DB trigger

Service Bus + SQL Server + Azurite
  ├── Service Bus queue trigger
  └── Service Bus topic trigger

SQL Server + Azurite
  └── SQL trigger
```

If a required emulator is already running under the repository's Compose
project, the runner reuses it and leaves it running. Containers created by the
runner are removed after diagnostics are captured.

## Azure DevOps integration

Azure DevOps runs the same integration-test command in a Linux host job for
pull requests:

```bash
cd tests/integration
go run ./cmd/integrationtest
```

The pipeline provisions Go, the pinned Core Tools release, and Docker before
invoking the runner. Pipeline YAML owns agent and tool provisioning only;
emulator lifecycle, readiness, test selection, diagnostics, and cleanup remain
implemented by the repository runner.

The integration-test job is a required pre-merge PR check. Azure DevOps
publishes the JUnit results and the complete artifact directory even when the
runner fails, so host and emulator diagnostics are available from the failed
build.

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
files for the same run and test names are replaced.

```text
artifacts/
├── run.json
├── go-test.xml
├── go-test.log
├── profiles/
│   ├── azurite.log
│   ├── eventhubs.log
│   ├── cosmosdb.log
│   ├── servicebus.log
│   └── sqlserver.log
└── hosts/
    ├── TestHttpTriggerGet/
    │   └── httpTrigger-host.log
    └── <test-name>/
        └── <sample-name>-host.log
```

- `run.json` records tool versions, emulator image digests, timestamps, and the
  source revision.
- `go-test.xml` contains JUnit test results.
- `go-test.log` contains complete Go test output.
- `profiles/*.log` contains complete emulator output.
- `hosts/*/*.log` contains complete Functions host and worker output.

On failure, the runner captures container status and logs before cleanup. A
cleanup error is reported without replacing the original test failure.

## Architecture

This directory is a separate Go module so test-only Azure SDK dependencies do
not affect the worker library's dependency tree.

```text
cmd/integrationtest
        |
        v
internal/testrunner
  - validates tools
  - owns emulator profiles
  - waits for readiness
  - runs Go tests
  - captures artifacts
  - performs cleanup
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

- The **runner** owns shared tools, emulator lifecycle, readiness, suite
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
| Azurite Table | `127.0.0.1:10002` | Table service request |
| Event Hubs | `127.0.0.1:5672` | Management health endpoint |
| Cosmos DB | `127.0.0.1:8081` | `http://127.0.0.1:8080/ready` |
| Service Bus | `127.0.0.1:5672` | `http://127.0.0.1:5300/health` |
| SQL Server | `127.0.0.1:1433` | `SELECT 1` |

Emulator images are pinned in `tests/emulators/docker-compose.yml`. Update image
versions deliberately and validate the full affected profile after each update.
