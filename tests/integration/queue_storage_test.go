package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
)

var queueStorageEnv = map[string]string{
	"AzureWebJobsStorage":            azuriteConnStr,
	"FUNCTIONS_WORKER_RUNTIME":       "native",
	"FUNCTIONS_CLI_NATIVE_LANGUAGE":  "go",
}

func TestQueueStorageTriggerFires(t *testing.T) {
	requireAzurite(t)

	// Create the queue in Azurite before starting the host
	ensureQueue(t, "myqueue")

	proc := StartFuncHost(t, "queueTrigger", 7210, queueStorageEnv, 30*time.Second)

	// Enqueue a message
	enqueueMessage(t, "myqueue", "Hello from queue storage integration test!")

	proc.AssertLogContains("queue trigger executed", 15*time.Second)
	proc.AssertLogContains("Succeeded", 5*time.Second)
}

func TestQueueStorageTriggerMetadata(t *testing.T) {
	requireAzurite(t)

	ensureQueue(t, "myqueue")

	proc := StartFuncHost(t, "queueTrigger", 7211, queueStorageEnv, 30*time.Second)

	enqueueMessage(t, "myqueue", "metadata-test-message")

	// Verify metadata fields are populated (logged by the sample handler)
	proc.AssertLogContains("queue trigger executed", 15*time.Second)
	proc.AssertLogContains("metadata-test-message", 5*time.Second)
}

// ensureQueue deletes any existing queue (clearing stale messages) and
// recreates it fresh for the test.
func ensureQueue(t *testing.T, queueName string) {
	t.Helper()
	client, err := azqueue.NewServiceClientFromConnectionString(azuriteConnStr, nil)
	if err != nil {
		t.Fatalf("failed to create queue service client: %v", err)
	}
	// Delete first to clear stale messages from prior runs
	_, _ = client.DeleteQueue(context.Background(), queueName, nil)
	_, err = client.CreateQueue(context.Background(), queueName, nil)
	if err != nil {
		t.Fatalf("failed to create queue %q: %v", queueName, err)
	}
}

// enqueueMessage sends a message to the specified queue.
func enqueueMessage(t *testing.T, queueName, body string) {
	t.Helper()
	client, err := azqueue.NewServiceClientFromConnectionString(azuriteConnStr, nil)
	if err != nil {
		t.Fatalf("failed to create queue service client: %v", err)
	}
	queueClient := client.NewQueueClient(queueName)
	_, err = queueClient.EnqueueMessage(context.Background(), fmt.Sprintf("%s", body), nil)
	if err != nil {
		t.Fatalf("failed to enqueue message: %v", err)
	}
}
