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

## Current coverage

| Sample | Integration pipeline coverage | Notes |
|---|---|---|
| `blobTrigger` | Partial | A test-only polling fixture validates Blob invocation and client creation. Event Grid delivery requires deployed Azure test resources. |
| `cosmosDBTrigger` | Full | Runs against the local Cosmos DB emulator. |
| `eventGridTrigger` | Partial | Verifies build, registration, and host loading; there is no local Event Grid emulator. |
| `eventHubTrigger` | Full | Tests single-event and batch delivery. |
| `httpTrigger` | Full | Tests GET and POST invocation. |
| `queueTrigger` | Full | Tests message delivery and metadata. |
| `serviceBusQueueTrigger` | Full | Tests single-message and batch delivery. |
| `serviceBusTopicTrigger` | Full | Tests single-message and batch delivery. |
| `sqlTrigger` | Full | Tests insert, update, and delete changes. |
| `timerTrigger` | Full | Tests scheduled invocation. |
| `httpStreaming` | Not covered | Requires a streaming-specific integration scenario. |
| `middleware` | Not covered | Requires an end-to-end middleware scenario. |
| `otelTracing` | Not covered | Requires telemetry capture and assertion infrastructure. |
| `collectorToAzureMonitor` | Not covered | Requires Azure Monitor or a test telemetry destination. |

The batch tests verify payload and per-message metadata alignment.

Run it from the integration-test module:

```bash
cd tests/integration
go run ./cmd/integrationtest
```

The runner executes these steps in order:

1. Validates Core Tools 4.12.0 or later and Docker Compose.
2. Creates the `artifacts` directory.
3. Inspects the Azurite, SQL Server, Service Bus, Event Hubs, and Cosmos DB
   emulator containers to determine ownership and running state.
4. Starts missing or stopped emulator services and waits for each required
   endpoint to become ready.
5. Runs every black-box scenario via one explicit `go test` pattern.
6. Captures every emulator log and the Go test log as artifacts.
7. Reaps any Functions hosts left behind if the Go test process is terminated.
8. Removes only containers that did not exist before the run. Pre-existing
   stopped containers that the runner starts are left in place afterward.

## Durable Functions coverage

`TestDurableOrchestrations` runs the `durableFunctions` sample against a real
host and covers the whole Durable Functions path: the `durableClient` binding
delivering the management endpoint to the worker, the gRPC management client,
orchestration replay through the trigger pipeline, activity dispatch and input
decoding, custom status, and external events.

The subtests share one host and cover a sequential orchestration, fan-out/fan-in
with automatic approval, human approval and rejection through an external event,
rejection on failed validation, and an unknown instance lookup.

```bash
cd tests/integration
go test -run TestDurableOrchestrations -v .
```

Azurite is the only emulator required. Every assertion goes through the sample's
own anonymous HTTP endpoints rather than the host's management API, so the same
subtests run unchanged against a deployed app:

```bash
DURABLE_E2E_BASE_URL=https://myapp.azurewebsites.net \
  go test -run TestDurableOrchestrations -v .
```

When `DURABLE_E2E_BASE_URL` is set the suite skips the local host and the Azurite
check and drives the given app instead.

### Host extension requirement

Durable Functions on the Go worker needs a DurableTask extension that recognizes
`FUNCTIONS_WORKER_RUNTIME=native` and selects the gRPC durable protocol. Without
it the starter endpoints return HTTP 500, either because no durable client
reaches the worker or because the extension fell back to its legacy local HTTP
RPC endpoint and the client's gRPC handshake fails against it. The test detects
both symptoms and fails with an explanation rather than a bare status code.

The fix shipped in DurableTask extension 3.15.0. The `durableFunctions` sample
pins the experimental bundle, which is the first bundle that resolves to 3.15.0
or later:

```json
"extensionBundle": {
  "id": "Microsoft.Azure.Functions.ExtensionBundle.Experimental",
  "version": "[4.7.0, 5.0.0)"
}
```

The experimental bundle is published to the same default CDN as the standard
bundle, so no `FUNCTIONS_EXTENSIONBUNDLE_SOURCE_URI` override is needed. Once a
standard bundle that resolves to 3.15.0 or later is listed in its CDN index, the
sample can move back to `Microsoft.Azure.Functions.ExtensionBundle`.

## Azure DevOps integration

The public Azure DevOps pipeline runs the complete integration suite for pull
requests targeting `main` and for pushes to `main`. The Linux job provisions Go
and Core Tools, verifies Docker, and invokes:

```bash
cd tests/integration
go run ./cmd/integrationtest
```

The pipeline job is defined in
`eng/ci/templates/jobs/integration-tests.yml` and included by
`eng/ci/public-build.yml`. Pipeline YAML owns agent and tool provisioning only;
emulator lifecycle, readiness, test selection, diagnostics, and cleanup remain
implemented by the repository runner.

Integration-test failures fail the public pipeline. Repository administrators
must configure the `golang-worker.public-build` status as a required `main`
branch protection check to prevent merging a pull request whose suite has not
passed. The job publishes `integration-test-artifacts` on every run for
diagnostics.

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
├── cosmosdb-emulator.log
├── eventhub-emulator.log
├── go-test.log
├── servicebus-emulator.log
├── sqlserver.log
└── TestScenario/
    └── sample-host.log
```

- Each emulator log contains the complete output from that Docker Compose
  service.
- `go-test.log` contains complete Go test output.
- Each scenario directory contains complete Functions host and worker output
  for its sample.

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
  - manages emulator lifecycle (start/reuse/cleanup)
  - waits for each emulator's readiness
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

- The **runner** owns shared tools, emulator lifecycle, readiness, test
  selection, artifacts, and cleanup.
- `TestHost` owns one Functions host process and its Go worker children.
- A **test scenario** owns only resource setup, stimulus, and behavioral
  assertions.

Tests do not start Docker containers, install tools, or own emulator cleanup.

## Emulator endpoints

| Emulator | Host endpoint | Readiness |
|---|---|---|
| Azurite Blob | `127.0.0.1:10000` | Blob service request |
| Azurite Queue | `127.0.0.1:10001` | Queue service request |
| SQL Server | `127.0.0.1:1433` | Authenticated database ping |
| Service Bus | `127.0.0.1:5672` | TCP connection |
| Event Hubs | `127.0.0.2:5672` | TCP connection |
| Cosmos DB | `127.0.0.1:8081` | HTTP service request |

Emulator images are pinned to immutable manifest digests in
`tests/emulators/docker-compose.yml`. Update image versions deliberately and
validate the affected tests after each update.

## Future work

The following are planned but not implemented:

- **JUnit output**: `go-test.xml` for Azure DevOps test result publishing.
- **Run metadata**: `run.json` recording tool versions, emulator image digests,
  timestamps, and source revision.
