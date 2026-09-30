# Durable Functions Integration Fixture

This test-only application validates Durable Functions end to end: the
`durableClient` binding, the gRPC management client, orchestration replay,
activity dispatch and input decoding, custom status, and external events.

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
later, which recognizes the native worker runtime. It also uses its own task hub
so fixture runs do not share state with the sample.
