package main

import (
	"context"
	"log"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/azure/azure-functions-golang-worker/sdk"
	_ "github.com/azure/azure-functions-golang-worker/sdk/extensions/eventhub"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// EventHubHandler handles EventHub trigger events using SDK types
func EventHubHandler(client *azeventhubs.ConsumerClient) {
	log.Printf("EventHub Trigger Executed")

	// Get Event Hub properties
	props, err := client.GetEventHubProperties(context.Background(), nil)
	if err != nil {
		log.Printf("Error getting Event Hub properties: %v", err)
		return
	}

	log.Printf("Event Hub: %s, Partitions: %v", props.Name, props.PartitionIDs)
}

func main() {
	app := sdk.FunctionApp()

	app.EventHub("eventHubTrigger", EventHubHandler).
		EventHubName("myeventhub").
		Connection("EventHubConnection").
		ConsumerGroup("$Default")

	worker.Start(app)
}
