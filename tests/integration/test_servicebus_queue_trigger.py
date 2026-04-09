"""Integration test for Service Bus Queue trigger sample."""

from azure.servicebus import ServiceBusClient, ServiceBusMessage

SB_CONN_STR = (
    "Endpoint=sb://localhost;"
    "SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE;"
    "UseDevelopmentEmulator=true;"
)

ENV = {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "golang",
    "ServiceBusConnection": SB_CONN_STR,
}


def test_servicebus_queue_trigger_fires(func_host):
    """Send a message to the Service Bus queue emulator and verify the trigger fires."""
    proc = func_host("serviceBusQueueTrigger", 7205, ENV)

    # Send a message
    client = ServiceBusClient.from_connection_string(SB_CONN_STR)
    sender = client.get_queue_sender("input-queue")
    with sender:
        sender.send_messages(ServiceBusMessage("Hello from SB queue integration test!"))

    proc.assert_log_contains("Service Bus Queue Trigger Executed", timeout=15)
    proc.assert_log_contains("Body: Hello from SB queue integration test!", timeout=5)
    proc.assert_log_contains("Succeeded", timeout=5)
