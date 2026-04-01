package main

import (
	"context"
	"log"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// EventHubHandler handles EventHub trigger events using a typed data struct.
func EventHubHandler(ctx context.Context, event bindings.EventHubMessage) error {
	log.Printf("EventHub Trigger Executed")
	log.Printf("Body: %s", event.Body)
	log.Printf("Sequence Number: %d", event.SequenceNumber)
	log.Printf("Offset: %s", event.Offset)
	log.Printf("Enqueued Time: %s", event.EnqueuedTimeUtc)

	if event.PartitionKey != "" {
		log.Printf("Partition Key: %s", event.PartitionKey)
	}

	return nil
}

func main() {
	app := sdk.FunctionApp()

	app.EventHub("eventHubTrigger", EventHubHandler).
		EventHubName("myeventhub").
		Connection("EventHubConnection").
		ConsumerGroup("$Default")

	worker.Start(app)
}
