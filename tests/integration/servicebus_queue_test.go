package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

const sbConnStr = "Endpoint=sb://localhost;" +
	"SharedAccessKeyName=RootManageSharedAccessKey;" +
	"SharedAccessKey=SAS_KEY_VALUE;" +
	"UseDevelopmentEmulator=true;"

var serviceBusQueueEnv = map[string]string{
	"AzureWebJobsStorage":  "UseDevelopmentStorage=true",
	"ServiceBusConnection": sbConnStr,
}

func TestServiceBusQueueTriggerFires(t *testing.T) {
	requireAzurite(t)
	requireServiceBus(t)
	host := startSampleHost(t, "serviceBusQueueTrigger", serviceBusQueueEnv, 30*time.Second)

	body := fmt.Sprintf("single-servicebus-%d", time.Now().UnixNano())
	sendServiceBusMessages(t, "input-queue", body)

	assertHostLogContains(t, host, "servicebus queue trigger executed", 15*time.Second)
	assertHostLogContains(t, host, body, 5*time.Second)
	assertHostLogContains(t, host, "test_id="+body, 5*time.Second)
	assertHostLogContains(t, host, "Executed 'Functions.queueFunc' (Succeeded", 5*time.Second)
}

func TestServiceBusQueueTriggerMany(t *testing.T) {
	requireAzurite(t)
	requireServiceBus(t)
	host := startSampleHost(t, "serviceBusQueueTrigger", serviceBusQueueEnv, 30*time.Second)

	runID := time.Now().UnixNano()
	bodies := []string{
		fmt.Sprintf("batch-servicebus-%d-1", runID),
		fmt.Sprintf("batch-servicebus-%d-2", runID),
	}
	sendServiceBusMessages(t, "input-queue-batch", bodies...)

	assertHostLogContains(t, host, "servicebus queue batch trigger executed", 20*time.Second)
	assertHostLogContains(t, host, "batch_size=2", 5*time.Second)
	for _, body := range bodies {
		assertHostLogContains(t, host, "alignment_key="+body+"|"+body+"|"+body, 5*time.Second)
	}
	assertHostLogContains(t, host, "Executed 'Functions.queueBatchFunc' (Succeeded", 5*time.Second)
}

func sendServiceBusMessages(t *testing.T, entity string, bodies ...string) {
	t.Helper()
	client, err := azservicebus.NewClientFromConnectionString(sbConnStr, nil)
	if err != nil {
		t.Fatalf("failed to create service bus client: %v", err)
	}
	defer client.Close(context.Background())

	sender, err := client.NewSender(entity, nil)
	if err != nil {
		t.Fatalf("failed to create sender: %v", err)
	}
	defer sender.Close(context.Background())

	batch, err := sender.NewMessageBatch(context.Background(), nil)
	if err != nil {
		t.Fatalf("failed to create message batch: %v", err)
	}
	for _, body := range bodies {
		if err := batch.AddMessage(&azservicebus.Message{
			Body:                  []byte(body),
			MessageID:             &body,
			ApplicationProperties: map[string]any{"testID": body},
		}, nil); err != nil {
			t.Fatalf("failed to add message to batch: %v", err)
		}
	}
	if err := sender.SendMessageBatch(context.Background(), batch, nil); err != nil {
		t.Fatalf("failed to send messages: %v", err)
	}
}
