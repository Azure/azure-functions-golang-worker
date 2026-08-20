package integration

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
)

var queueStorageEnv = map[string]string{
	"AzureWebJobsStorage": azuriteConnStr,
}

func TestQueueStorageTriggerFires(t *testing.T) {
	requireAzurite(t)

	// Create the queue in Azurite before starting the host
	ensureQueue(t, "myqueue")

	host := startSampleHost(t, "queueTrigger", queueStorageEnv, 30*time.Second)

	// Enqueue a message
	enqueueMessage(t, "myqueue", "Hello from queue storage integration test!")

	assertHostLogContains(t, host, "queue trigger executed", 15*time.Second)
	assertHostLogContains(t, host, "Succeeded", 5*time.Second)
}

func TestQueueStorageTriggerMetadata(t *testing.T) {
	requireAzurite(t)

	ensureQueue(t, "myqueue")

	host := startSampleHost(t, "queueTrigger", queueStorageEnv, 30*time.Second)

	enqueueMessage(t, "myqueue", "metadata-test-message")

	// Verify metadata fields are populated (logged by the sample handler)
	assertHostLogContains(t, host, "queue trigger executed", 15*time.Second)
	assertHostLogContains(t, host, "metadata-test-message", 5*time.Second)
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
	// Azure Functions decodes Storage Queue messages from base64 before
	// delivering them to the function. Encode the test payload so the handler
	// receives the original readable text instead of the message being poisoned.
	encodedBody := base64.StdEncoding.EncodeToString([]byte(body))
	_, err = queueClient.EnqueueMessage(context.Background(), encodedBody, nil)
	if err != nil {
		t.Fatalf("failed to enqueue message: %v", err)
	}
}
