"""Integration test for Event Grid trigger sample.

Note: Event Grid has no local emulator. This test only verifies
that the function builds, registers, and loads correctly.
"""

ENV = {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "golang",
}


def test_eventgrid_trigger_registers(func_host):
    """Verify Event Grid trigger function builds, registers, and loads."""
    proc = func_host("eventGridTrigger", 7209, ENV)

    proc.assert_log_contains("EventGridHandler", timeout=10)
    log = proc.read_log()
    assert "error" not in log.lower() or "Unable to resolve ScriptJobHostOptions" in log
