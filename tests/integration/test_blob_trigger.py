"""Integration test for Blob trigger sample."""

from azure.storage.blob import BlobServiceClient

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
}


def test_blob_trigger_fires(func_host):
    """Upload a blob to Azurite and verify the trigger fires."""
    proc = func_host("blobTrigger", 7204, ENV)

    # Create container and upload blob
    blob_service = BlobServiceClient.from_connection_string(
        AZURITE_CONN_STR, api_version="2024-08-04"
    )
    container_client = blob_service.get_container_client("test-container")
    try:
        container_client.create_container()
    except Exception:
        pass  # Already exists

    import uuid
    blob_name = f"test-{uuid.uuid4().hex[:8]}.txt"
    blob_content = "Hello from blob trigger integration test!"
    container_client.upload_blob(blob_name, blob_content, overwrite=True)

    # Blob trigger uses polling — can take up to 60 seconds
    proc.assert_log_contains("Blob Trigger Executed", timeout=90)
    proc.assert_log_contains("Succeeded", timeout=5)
