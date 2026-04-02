"""Integration test for Service Bus Topic trigger sample."""

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


def test_servicebus_topic_trigger_fires(func_host):
    """Send a message to the Service Bus topic emulator and verify the trigger fires."""
    proc = func_host("serviceBusTopicTrigger", 7206, ENV)

    # Send a message to the topic
    client = ServiceBusClient.from_connection_string(SB_CONN_STR)
    sender = client.get_topic_sender("orders")
    with sender:
        sender.send_messages(ServiceBusMessage("Order #99 from topic integration test"))

    proc.assert_log_contains("Service Bus Topic Trigger Executed", timeout=15)
    proc.assert_log_contains("Body: Order #99 from topic integration test", timeout=5)
    proc.assert_log_contains("Succeeded", timeout=5)
