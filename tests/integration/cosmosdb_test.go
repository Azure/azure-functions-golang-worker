package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

const (
	cosmosEndpoint = "http://127.0.0.1:8081/"
	cosmosKey      = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
	cosmosConnStr  = "AccountEndpoint=" + cosmosEndpoint + ";AccountKey=" + cosmosKey
)

var cosmosEnv = map[string]string{
	"AzureWebJobsStorage": "UseDevelopmentStorage=true",
	"CosmosDBConnection":  cosmosConnStr,
}

func ensureCosmosContainers(t *testing.T) {
	t.Helper()

	cred, err := azcosmos.NewKeyCredential(cosmosKey)
	if err != nil {
		t.Fatalf("failed to create cosmos credential: %v", err)
	}
	client, err := azcosmos.NewClientWithKey(cosmosEndpoint, cred, nil)
	if err != nil {
		t.Fatalf("failed to create cosmos client: %v", err)
	}

	ctx := context.Background()

	// Create database
	_, err = client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: "ToDoList"}, nil)
	if err != nil && !isCosmosConflict(err) {
		t.Fatalf("failed to create database: %v", err)
	}

	db, err := client.NewDatabase("ToDoList")
	if err != nil {
		t.Fatalf("failed to get database client: %v", err)
	}

	// Create monitored container
	_, err = db.CreateContainer(ctx, azcosmos.ContainerProperties{
		ID: "Items",
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths: []string{"/id"},
		},
	}, nil)
	if err != nil && !isCosmosConflict(err) {
		t.Fatalf("failed to create Items container: %v", err)
	}

	// Delete stale leases so the trigger re-reads the change feed from scratch
	leaseContainer, err := db.NewContainer("leases")
	if err == nil {
		_, _ = leaseContainer.Delete(ctx, nil)
	}

	// Re-create leases container
	_, err = db.CreateContainer(ctx, azcosmos.ContainerProperties{
		ID: "leases",
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths: []string{"/id"},
		},
	}, nil)
	if err != nil && !isCosmosConflict(err) {
		t.Fatalf("failed to create leases container: %v", err)
	}
}

func isCosmosConflict(err error) bool {
	// azcosmos returns *azcore.ResponseError with StatusCode 409 for conflicts
	return err != nil && (contains(err.Error(), "409") || contains(err.Error(), "Conflict"))
}

func TestCosmosDBTriggerFires(t *testing.T) {
	requireAzurite(t)
	requireCosmosDB(t)
	ensureCosmosContainers(t)

	proc := StartFuncHost(t, "cosmosDBTrigger", 7208, cosmosEnv, 40*time.Second)

	// Wait for the change feed listener to acquire leases
	proc.AssertLogContains("Started the listener", 60*time.Second)

	// Insert a document
	cred, err := azcosmos.NewKeyCredential(cosmosKey)
	if err != nil {
		t.Fatalf("failed to create cosmos credential: %v", err)
	}
	client, err := azcosmos.NewClientWithKey(cosmosEndpoint, cred, nil)
	if err != nil {
		t.Fatalf("failed to create cosmos client: %v", err)
	}

	container, err := client.NewContainer("ToDoList", "Items")
	if err != nil {
		t.Fatalf("failed to get container client: %v", err)
	}

	docID := fmt.Sprintf("test-%d", time.Now().UnixNano())
	doc := map[string]string{
		"id":   docID,
		"data": "Hello from Cosmos integration test!",
	}
	docBytes, _ := json.Marshal(doc)
	pk := azcosmos.NewPartitionKeyString(docID)

	_, err = container.CreateItem(context.Background(), pk, docBytes, nil)
	if err != nil {
		t.Fatalf("failed to insert cosmos document: %v", err)
	}

	// Cosmos change feed can take 10-30s to detect changes
	proc.AssertLogContains("Executing 'Functions.docs'", 45*time.Second)
	proc.AssertLogContains("Succeeded", 10*time.Second)
}
