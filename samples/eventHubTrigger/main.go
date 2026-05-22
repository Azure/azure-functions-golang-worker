package main

import (
	"context"
	"log/slog"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// EventHubHandler handles EventHub trigger events using a typed data struct.
func EventHubHandler(ctx context.Context, event bindings.EventHubMessage) error {
	slog.InfoContext(ctx, "eventhub trigger executed",
		"body", string(event.Body),
		"sequence_number", event.SequenceNumber,
		"offset", event.Offset,
		"enqueued_time", event.EnqueuedTimeUtc,
		"partition_key", event.PartitionKey,
	)
	return nil
}

func main() {
	app := sdk.FunctionApp()

	app.EventHub("eventHubTrigger", EventHubHandler,
		sdk.WithEventHubName("input-hub"),
		sdk.WithConnection("EventHubConnection"),
		sdk.WithConsumerGroup("watchtower-test"),
	)

	worker.Start(app)
}
