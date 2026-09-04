# Durable Functions Integration Fixture

This test-only application validates Durable Functions end to end: the
`durableClient` binding, the gRPC management client, orchestration replay,
activity dispatch and input decoding, custom status, and external events.

It mirrors `samples/durableFunctions` without the OpenTelemetry wiring, which
the assertions do not exercise and which would otherwise pull the collector and
its exporters into this module.

The fixture has its own Go module with local replacements for the worker and the
durable middleware, so both local runs and CI build the current checkout.

`host.json` pins the experimental extension bundle, currently the only bundle
whose CDN index resolves to a DurableTask extension that recognizes the native
worker runtime. It also uses its own task hub so fixture runs do not share state
with the sample.
