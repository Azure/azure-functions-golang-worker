package integration

import (
	"fmt"
	"testing"
	"time"
)

var serviceBusTopicEnv = map[string]string{
	"AzureWebJobsStorage":  "UseDevelopmentStorage=true",
	"ServiceBusConnection": sbConnStr,
}

func TestServiceBusTopicTriggerFires(t *testing.T) {
	requireAzurite(t)
	requireServiceBus(t)
	host := startSampleHost(t, "serviceBusTopicTrigger", serviceBusTopicEnv, 30*time.Second)

	body := fmt.Sprintf("single-servicebus-topic-%d", time.Now().UnixNano())
	sendServiceBusMessages(t, "orders", body)

	assertHostLogContains(t, host, "servicebus topic trigger executed", 15*time.Second)
	assertHostLogContains(t, host, body, 5*time.Second)
	assertHostLogContains(t, host, "test_id="+body, 5*time.Second)
	assertHostLogContains(t, host, "Executed 'Functions.topicFunc' (Succeeded", 5*time.Second)
}

func TestServiceBusTopicTriggerMany(t *testing.T) {
	requireAzurite(t)
	requireServiceBus(t)
	host := startSampleHost(t, "serviceBusTopicTrigger", serviceBusTopicEnv, 30*time.Second)

	runID := time.Now().UnixNano()
	bodies := []string{
		fmt.Sprintf("batch-servicebus-topic-%d-1", runID),
		fmt.Sprintf("batch-servicebus-topic-%d-2", runID),
	}
	sendServiceBusMessages(t, "orders-batch", bodies...)

	assertHostLogContains(t, host, "servicebus topic batch trigger executed", 20*time.Second)
	assertHostLogContains(t, host, "batch_size=2", 5*time.Second)
	for _, body := range bodies {
		assertHostLogContains(t, host, "alignment_key="+body+"|"+body+"|"+body, 5*time.Second)
	}
	assertHostLogContains(t, host, "Executed 'Functions.topicBatchFunc' (Succeeded", 5*time.Second)
}
