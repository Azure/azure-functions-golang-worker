"""Integration test for Timer trigger sample."""

ENV = {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "golang",
}


def test_timer_trigger_fires(func_host):
    """Timer trigger fires every 10 seconds. Wait and verify invocation."""
    proc = func_host("timerTrigger", 7203, ENV)

    # The timer schedule is */10 * * * * * (every 10 seconds)
    proc.assert_log_contains("Timer trigger executed", timeout=20)
    proc.assert_log_contains("Succeeded", timeout=5)
