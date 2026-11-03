# Durable Functions Integration Fixture

This test-only application validates Durable Functions end to end: the
`durableClient` binding, the gRPC management client, orchestration replay,
activity dispatch and input decoding, custom status, and external events.
`lifecycle.go` adds test-only HTTP probes for the public Go management client's
schedule, fetch, raise, and blocking wait methods. Its bounded event waits
exercise real event buffering and timer expiration. It contains no scheduler.

It mirrors `samples/durableFunctions` without an embedded collector.
`DURABLE_TEST_MIDDLEWARE_ORDER=before` or `after` enables the ordering probe with
real `otelfunc` middleware and an in-memory exporter. The probe covers replay,
ContinueAsNew, timers, sub-orchestrations, failure, panic recovery, deliberate
short-circuits, late replay-input replacement, and context propagation in both
registration orders. The input probe rewrites a valid first-turn envelope from
0 to 1 to 42 in two middleware and checks the orchestrator returns 42, not the
pre-bound 0. It is a protocol test, not application guidance for editing history.
Its HTTP snapshot contains only test observations, not binding credentials.

The fixture has its own Go module with local replacements for the worker and the
durable middleware, so both local runs and CI build the current checkout.

`host.json` pins the experimental extension bundle with DurableTask 3.15.0 or
later, which recognizes the native worker runtime. The integration helper
overrides its default task hub and host ID with unique test-owned values and
uses the private `durable-azurite` service on ports 17310–17312. It does not delete
shared host checkpoint containers. Restart coverage preserves the hub and
storage while replacing only the owned host process. See the integration
README for local startup, deployed-fixture use, timeout bounds, and log paths.

Set `DURABLE_TEST_BLOB_CONNECTION_STRING` to opt into the Azure Blob payload
store. The fixture then registers a large-payload orchestration that round-trips
large inputs and outputs through activities, sub-orchestrations, external events,
custom status, entity state, and a `ContinueAsNew` input. Small string and object
activity values remain inline controls. The corresponding integration test
starts an isolated task hub and verifies application values and native blob
tokens. The test host explicitly uses the supplied connection string for its
durable state store as well.
Without the variable, payload offloading and its test-only endpoints are not
registered.
