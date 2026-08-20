# Blob Trigger Integration Fixture

This test-only application validates Blob trigger invocation and Blob client
creation against Azurite.

Azurite does not emit Event Grid notifications, so the fixture uses the polling
Blob source. It is intentionally separate from the customer-facing sample,
which demonstrates the production Event Grid configuration.

All other integration scenarios run their corresponding applications from the
repository's `samples` directory directly.
