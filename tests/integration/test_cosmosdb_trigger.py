"""Integration test for Cosmos DB trigger sample."""

from azure.cosmos import CosmosClient, PartitionKey

COSMOS_ENDPOINT = "http://localhost:8081/"
COSMOS_KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
COSMOS_CONN_STR = f"AccountEndpoint={COSMOS_ENDPOINT};AccountKey={COSMOS_KEY}"

ENV = {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "golang",
    "CosmosDBConnection": COSMOS_CONN_STR,
}


def _ensure_cosmos_containers():
    """Create the database, monitored container, and leases container if they don't exist."""
    client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
    db = client.create_database_if_not_exists("ToDoList")
    db.create_container_if_not_exists("Items", partition_key=PartitionKey(path="/id"))
    db.create_container_if_not_exists("leases", partition_key=PartitionKey(path="/id"))


def test_cosmosdb_trigger_fires(func_host):
    """Insert a document into Cosmos DB and verify the change feed trigger fires."""
    _ensure_cosmos_containers()

    proc = func_host("cosmosDBTrigger", 7208, ENV, init_timeout=40)

    # Wait for the change feed listener to acquire leases
    proc.assert_log_contains("Started the listener", timeout=60)

    # Insert a document
    import uuid
    client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
    container = client.get_database_client("ToDoList").get_container_client("Items")
    doc_id = str(uuid.uuid4())
    container.create_item({"id": doc_id, "data": "Hello from Cosmos integration test!"})

    # Cosmos change feed can take 10-30s to detect changes
    proc.assert_log_contains("Executing 'Functions.docs'", timeout=45)
    proc.assert_log_contains("Succeeded", timeout=10)
