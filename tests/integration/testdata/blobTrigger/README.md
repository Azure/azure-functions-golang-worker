# Blob Trigger Integration Fixture

This test-only application validates Blob trigger invocation and Blob client
creation against Azurite.

Azurite does not emit Event Grid notifications, so the fixture uses the polling
Blob source. It is intentionally separate from the customer-facing sample,
which demonstrates the production Event Grid configuration.

The fixture has its own Go module with local replacements for the worker and
Blob trigger modules, so both local runs and CI build the current checkout.

Integration scenarios whose application only needs the root module run from the
repository's `samples` directory directly.
