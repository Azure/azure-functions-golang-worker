"""Integration test for Event Hub trigger sample."""

import time

from azure.eventhub import EventHubProducerClient, EventData

EH_CONN_STR = (
    "Endpoint=sb://127.0.0.2;"
    "SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;"
    "UseDevelopmentEmulator=true;"
)

AZURITE_CONN_STR = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    "QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"
    "TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
)

ENV = {
    "AzureWebJobsStorage": AZURITE_CONN_STR,
    "FUNCTIONS_WORKER_RUNTIME": "golang",
    "EventHubConnection": EH_CONN_STR,
}


def _send_event(message: str):
    """Send a single event to the Event Hub emulator."""
    producer = EventHubProducerClient.from_connection_string(EH_CONN_STR, eventhub_name="input-hub")
    with producer:
        batch = producer.create_batch()
        batch.add(EventData(message))
        producer.send_batch(batch)


def test_eventhub_trigger_fires(func_host):
    """Send events to the Event Hub emulator and verify the trigger fires.

    The EH emulator's partition claim process is slow and unpredictable.
    We send events repeatedly until the trigger fires, rather than sending
    once and waiting with a fixed timeout.
    """
    proc = func_host("eventHubTrigger", 7207, ENV, init_timeout=40)

    # Send events repeatedly — the listener may not be ready for the first few
    deadline = time.time() + 120  # 2 minute overall timeout
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        _send_event(f"EH integration test event #{attempt}")

        # Check if trigger already fired
        log = proc.read_log()
        if "EventHub Trigger Executed" in log:
            break

        time.sleep(10)  # Wait before next send

    proc.assert_log_contains("EventHub Trigger Executed", timeout=5)
    proc.assert_log_contains("Succeeded", timeout=5)
