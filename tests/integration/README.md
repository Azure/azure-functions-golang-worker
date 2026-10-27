# Integration Tests

Black-box integration tests for the Azure Functions Go Worker. The suite verifies
that Azure Functions Core Tools can start the Functions host, establish the
worker channel, index Go functions, invoke them, and return or process the
expected output.

The default runner uses local Docker emulators and does not require Azure
credentials. Durable-only runs can also use private local Azurite or a deployed
test fixture.

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
| `durableFunctions` | Full | Uses isolated Azurite storage for replay, lifecycle, management, restart, and middleware ordering checks. |
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
3. Inspects the shared and Durable-only Azurite, SQL Server, Service Bus,
   Event Hubs, and Cosmos DB emulator containers to determine ownership and
   running state.
4. Starts missing or stopped emulator services and waits for each required
   endpoint to become ready.
5. Runs every black-box scenario via one explicit `go test` pattern.
6. Captures every emulator log and the Go test log as artifacts.
7. Reaps any Functions hosts left behind if the Go test process is terminated.
8. Removes only containers that did not exist before the run. Pre-existing
   stopped containers that the runner starts are left in place afterward.

## Durable Functions coverage

`TestDurableOrchestrations` runs the `durableFunctions` fixture against a real
host and covers the whole Durable Functions path: the `durableClient` binding
delivering the management endpoint to the worker, the gRPC management client,
orchestration replay through the trigger pipeline, activity dispatch and input
decoding, custom status, and external events.

The subtests share one host and cover sequential activities, fan-out/fan-in,
automatic and human approval, rejection, failed validation, and missing instances.
Lifecycle probes additionally check custom status while waiting, external event
buffering, a short durable event timeout, and direct Go client schedule, fetch,
raise, wait-for-start, and wait-for-completion operations. A local-only test stops
the suite-owned host while an expense awaits approval, restarts it against the
same task hub and storage, and completes that same instance.

`TestDurableMiddlewareOrdering` starts a separate host for each middleware order.
It preserves the registered adapter, input replacement, child/ContinueAsNew,
timer, failure, panic, short-circuit, and trace-context assertions.

### Isolated local storage

Durable tests do not call the general trigger helper that deletes shared
`azure-webjobs-hosts` and `azure-webjobs-eventhub` containers. The runner starts
the dedicated `durable-azurite` Compose service on loopback ports 17310–17312.
Each host gets a fresh task hub and host ID. Restart preserves those IDs and
changes only the owned Core Tools process. No shared emulator is reconfigured,
restarted, or cleaned. Test storage is volatile and discarded when its private
emulator stops. A reused emulator can retain old test hubs until it is discarded.

For just the Durable suites:

```bash
docker compose -f tests/emulators/docker-compose.yml up -d durable-azurite
cd tests/integration
GOWORK=off go test -count=1 -timeout 12m \
  -run '^(TestDurableOrchestrations|TestDurableMiddlewareOrdering)$' -v .
```

Alternatively run a private local Azurite process with `--inMemoryPersistence
--skipApiVersionCheck --disableTelemetry --blobPort 17310 --queuePort 17311
--tablePort 17312`. `DURABLE_E2E_STORAGE_CONNECTION_STRING` can point at other
explicitly test-owned storage. It must not point at shared development storage.
`UseDevelopmentStorage=true` is rejected. No persistent machine settings are
changed.

The helper stages `host.json` and the built `bin/app` in a test-owned temporary
directory, so concurrent runs do not share an executable or leave build output
in the fixture checkout. Restart reuses that temporary app until host cleanup
finishes. It builds the fixture with `GOWORK=off`, starts Core Tools with
`--no-build`, and supplies the absolute native executable path, including
`.exe` on Windows. Startup, HTTP requests, management waits, and shutdown are
bounded. Host logs and process IDs are under `artifacts/<test>/<generation>`.

Azurite is the only emulator required. Assertions go through the fixture's
anonymous endpoints, not the host's HTTP management API. The lifecycle endpoints
call the public Go management client directly. To test a deployed **fixture**
(not the unmodified sample):

```bash
DURABLE_E2E_BASE_URL=https://myapp.azurewebsites.net \
  go test -run TestDurableOrchestrations -v .
```

When `DURABLE_E2E_BASE_URL` is set the suite skips the local host and the Azurite
startup path and drives the given fixture instead. Restart coverage is explicitly
skipped because the suite does not own that host. Ordering tests always use local,
suite-owned hosts.

### Replacements for the removed scheduler fake

- `TestEndToEnd_Emulator` greeting and completion assertions now live in
  `SequentialOrchestrationCompletes`, with real activity dispatch and replay.
- `TestEndToEnd_FanOutAndExternalEvent` and
  `TestClient_EndToEnd_GrpcSidecar` are covered by the expense scenarios and
  `DirectGoManagementOperations`. These preserve nonempty/explicit instance IDs,
  fan-out/fan-in results, start/completion waits, waiting custom status, event
  payloads, and completed approved output.
- Their early-event behavior is checked by `EarlyExternalEventIsNotLost`.
  `BufferedEvent` waits for `Release` before consuming the previously delivered
  `Continue` event. `ExternalEventTimeoutUsesDurableTimer` verifies the opposite
  path without fabricating a timer-fired history.
- Binding endpoint selection, schedule request encoding, fixed reply decoding,
  cached client identity, and SDK shutdown remain in
  `TestDurable_ClientBinding_ProtocolAndLifetime`. Lifecycle completion belongs
  to the real host suites, not that stub.
- `TestClient_FetchMetadata_NotFound` retains deterministic sentinel translation
  from a fixed `exists=false` reply. Both the HTTP missing-instance test and the
  direct Go client probe verify it against the actual extension.

Descriptor-based unit histories still test individual replay turns and schema
compatibility through the real upstream executor. Fixed gRPC stubs have no
registry, executor, scheduling loop, or mutable instance history. The composed
fixture logging tests remain unchanged and continue to test the registered
adapter, executor, SDK RpcLog writer, and OTel bridge.

### Host extension requirement

Durable Functions on the Go worker needs a DurableTask extension that recognizes
`FUNCTIONS_WORKER_RUNTIME=native` and selects the gRPC durable protocol. Without
it the starter endpoints return HTTP 500, either because no durable client
reaches the worker or because the extension fell back to its legacy local HTTP
RPC endpoint and the client's gRPC handshake fails against it. The test detects
both symptoms and fails with an explanation rather than a bare status code.

The fix shipped in DurableTask extension 3.15.0. The `durableFunctions` fixture
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
├── durable-azurite.log
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
