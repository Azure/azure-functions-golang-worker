package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/azure/azure-functions-golang-worker/tests/integration/internal/testhost"
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
	host := startSampleHost(t, "eventHubTrigger", eventHubEnv, 40*time.Second)
	body := fmt.Sprintf("single-eventhub-%d", time.Now().UnixNano())
	sendEventHubEventsUntilHandled(t, host, "input-hub", "eventhub trigger executed", body)

	assertHostLogContains(t, host, body, 5*time.Second)
	assertHostLogContains(t, host, "test_id="+body, 5*time.Second)
	assertHostLogContains(t, host, "Executed 'Functions.eventHubTrigger' (Succeeded", 5*time.Second)
}

func TestEventHubTriggerMany(t *testing.T) {
	requireAzurite(t)
	requireEventHub(t)
	host := startSampleHost(t, "eventHubTrigger", eventHubEnv, 40*time.Second)

	runID := time.Now().UnixNano()
	bodies := []string{
		fmt.Sprintf("batch-eventhub-%d-1", runID),
		fmt.Sprintf("batch-eventhub-%d-2", runID),
	}
	sendEventHubEventsUntilHandled(t, host, "input-hub-batch", "eventhub batch trigger executed", bodies...)

	assertHostLogContains(t, host, "batch_size=2", 5*time.Second)
	for _, body := range bodies {
		assertHostLogContains(t, host, body, 5*time.Second)
		assertHostLogContains(t, host, "test_id="+body, 5*time.Second)
	}
	assertHostLogContains(t, host, "Executed 'Functions.eventHubBatchTrigger' (Succeeded", 5*time.Second)
}

func sendEventHubEventsUntilHandled(t *testing.T, host testhost.Host, hub, handledPattern string, bodies ...string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(120 * time.Second)
	attempt := 0
	partitionID := "0"

sendAttempts:
	for time.Now().Before(deadline) {
		attempt++

		producer, err := azeventhubs.NewProducerClientFromConnectionString(ehConnStr, hub, nil)
		if err != nil {
			t.Fatalf("failed to create eventhub producer: %v", err)
		}

		batch, err := producer.NewEventDataBatch(ctx, &azeventhubs.EventDataBatchOptions{
			PartitionID: &partitionID,
		})
		if err != nil {
			producer.Close(ctx)
			t.Logf("attempt %d: failed to create batch: %v", attempt, err)
			time.Sleep(10 * time.Second)
			continue
		}

		for _, body := range bodies {
			if err = batch.AddEventData(&azeventhubs.EventData{
				Body:       []byte(body),
				Properties: map[string]any{"testID": body},
			}, nil); err != nil {
				producer.Close(ctx)
				t.Logf("attempt %d: failed to add event: %v", attempt, err)
				time.Sleep(10 * time.Second)
				continue sendAttempts
			}
		}

		err = producer.SendEventDataBatch(ctx, batch, nil)
		producer.Close(ctx)
		if err != nil {
			t.Logf("attempt %d: failed to send batch: %v", attempt, err)
			time.Sleep(10 * time.Second)
			continue
		}

		if hostLogContains(host, handledPattern, 10*time.Second) {
			return
		}
		log, readErr := os.ReadFile(host.LogPath())
		if readErr == nil && strings.Contains(string(log), "Exception binding parameter") {
			t.Fatalf("event hub invocation failed during host binding:\n%s", string(log))
		}
	}
	t.Fatalf("event hub handler log %q not observed before timeout", handledPattern)
}
