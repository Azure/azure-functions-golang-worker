package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
)

const ehConnStr = "Endpoint=sb://127.0.0.2;" +
	"SharedAccessKeyName=RootManageSharedAccessKey;" +
	"SharedAccessKey=SAS_KEY_VALUE;" +
	"UseDevelopmentEmulator=true;"

var eventHubEnv = map[string]string{
	"AzureWebJobsStorage": "UseDevelopmentStorage=true",
	"EventHubConnection":  ehConnStr,
}

func TestEventHubTriggerFires(t *testing.T) {
	requireAzurite(t)
	requireEventHub(t)
	proc := StartFuncHost(t, "eventHubTrigger", 7207, eventHubEnv, 40*time.Second)

	// Send events repeatedly — the listener may not be ready for the first few.
	// The EH emulator's partition claim process is slow and unpredictable.
	ctx := context.Background()
	deadline := time.Now().Add(120 * time.Second)
	attempt := 0

	for time.Now().Before(deadline) {
		attempt++

		producer, err := azeventhubs.NewProducerClientFromConnectionString(ehConnStr, "input-hub", nil)
		if err != nil {
			t.Fatalf("failed to create eventhub producer: %v", err)
		}

		batch, err := producer.NewEventDataBatch(ctx, nil)
		if err != nil {
			producer.Close(ctx)
			t.Logf("attempt %d: failed to create batch: %v", attempt, err)
			time.Sleep(10 * time.Second)
			continue
		}

		err = batch.AddEventData(&azeventhubs.EventData{
			Body: []byte(fmt.Sprintf("EH integration test event #%d", attempt)),
		}, nil)
		if err != nil {
			producer.Close(ctx)
			t.Logf("attempt %d: failed to add event: %v", attempt, err)
			time.Sleep(10 * time.Second)
			continue
		}

		err = producer.SendEventDataBatch(ctx, batch, nil)
		producer.Close(ctx)
		if err != nil {
			t.Logf("attempt %d: failed to send batch: %v", attempt, err)
			time.Sleep(10 * time.Second)
			continue
		}

		// Check if trigger already fired
		log := proc.ReadLog()
		if contains(log, "EventHub Trigger Executed") {
			break
		}

		time.Sleep(10 * time.Second)
	}

	proc.AssertLogContains("eventhub trigger executed", 5*time.Second)
	proc.AssertLogContains("Succeeded", 5*time.Second)
}
