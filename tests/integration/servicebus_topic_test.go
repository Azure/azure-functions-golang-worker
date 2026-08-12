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
