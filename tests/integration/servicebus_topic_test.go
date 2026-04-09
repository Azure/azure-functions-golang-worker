package integration

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

var serviceBusTopicEnv = map[string]string{
	"AzureWebJobsStorage":      "UseDevelopmentStorage=true",
	"FUNCTIONS_WORKER_RUNTIME": "golang",
	"ServiceBusConnection":     sbConnStr,
}

func TestServiceBusTopicTriggerFires(t *testing.T) {
	requireAzurite(t)
	requireServiceBus(t)
	proc := StartFuncHost(t, "serviceBusTopicTrigger", 7206, serviceBusTopicEnv, 30*time.Second)

	// Send a message to the topic
	client, err := azservicebus.NewClientFromConnectionString(sbConnStr, nil)
	if err != nil {
		t.Fatalf("failed to create service bus client: %v", err)
	}
	defer client.Close(context.Background())

	sender, err := client.NewSender("orders", nil)
	if err != nil {
		t.Fatalf("failed to create sender: %v", err)
	}
	defer sender.Close(context.Background())

	err = sender.SendMessage(context.Background(), &azservicebus.Message{
		Body: []byte("Order #99 from topic integration test"),
	}, nil)
	if err != nil {
		t.Fatalf("failed to send message: %v", err)
	}

	proc.AssertLogContains("Service Bus Topic Trigger Executed", 15*time.Second)
	proc.AssertLogContains("Body: Order #99 from topic integration test", 5*time.Second)
	proc.AssertLogContains("Succeeded", 5*time.Second)
}
