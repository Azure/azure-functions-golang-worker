package integration

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

const sbConnStr = "Endpoint=sb://localhost;" +
	"SharedAccessKeyName=RootManageSharedAccessKey;" +
	"SharedAccessKey=SAS_KEY_VALUE;" +
	"UseDevelopmentEmulator=true;"

var serviceBusQueueEnv = map[string]string{
	"AzureWebJobsStorage":      "UseDevelopmentStorage=true",
	"FUNCTIONS_WORKER_RUNTIME": "golang",
	"ServiceBusConnection":     sbConnStr,
}

func TestServiceBusQueueTriggerFires(t *testing.T) {
	requireAzurite(t)
	requireServiceBus(t)
	proc := StartFuncHost(t, "serviceBusQueueTrigger", 7205, serviceBusQueueEnv, 30*time.Second)

	// Send a message to the queue
	client, err := azservicebus.NewClientFromConnectionString(sbConnStr, nil)
	if err != nil {
		t.Fatalf("failed to create service bus client: %v", err)
	}
	defer client.Close(context.Background())

	sender, err := client.NewSender("input-queue", nil)
	if err != nil {
		t.Fatalf("failed to create sender: %v", err)
	}
	defer sender.Close(context.Background())

	err = sender.SendMessage(context.Background(), &azservicebus.Message{
		Body: []byte("Hello from SB queue integration test!"),
	}, nil)
	if err != nil {
		t.Fatalf("failed to send message: %v", err)
	}

	proc.AssertLogContains("servicebus queue trigger executed", 15*time.Second)
	proc.AssertLogContains("Succeeded", 5*time.Second)
}
