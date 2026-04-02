"""Integration test for Event Hub trigger sample."""

from azure.eventhub import EventHubProducerClient, EventData

EH_CONN_STR = (
    "Endpoint=sb://127.0.0.2;"
    "SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;"
    "UseDevelopmentEmulator=true;"
)

ENV = {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "golang",
    "EventHubConnection": EH_CONN_STR,
}


def test_eventhub_trigger_fires(func_host):
    """Send an event to the Event Hub emulator and verify the trigger fires."""
    proc = func_host("eventHubTrigger", 7207, ENV, init_timeout=40)

    # Send an event
    producer = EventHubProducerClient.from_connection_string(EH_CONN_STR, eventhub_name="input-hub")
    with producer:
        batch = producer.create_batch()
        batch.add(EventData("Hello from EH integration test!"))
        producer.send_batch(batch)

    # EH emulator needs time to claim partitions and start processing
    proc.assert_log_contains("EventHub Trigger Executed", timeout=90)
    proc.assert_log_contains("Succeeded", timeout=10)
