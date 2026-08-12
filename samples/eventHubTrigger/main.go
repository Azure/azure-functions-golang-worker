package main

import (
	"context"
	"fmt"
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
		"test_id", event.Properties["testID"],
	)
	return nil
}

func EventHubBatchHandler(ctx context.Context, events []bindings.EventHubMessage) error {
	slog.InfoContext(ctx, "eventhub batch trigger executed", "batch_size", len(events))
	for _, event := range events {
		slog.InfoContext(ctx, "eventhub batch event",
			"body", string(event.Body),
			"sequence_number", event.SequenceNumber,
			"offset", event.Offset,
			"test_id", event.Properties["testID"],
			"alignment_key", fmt.Sprintf("%s|%v", event.Body, event.Properties["testID"]),
		)
	}
	return nil
}

func main() {
	app := sdk.FunctionApp()

	app.EventHub("eventHubTrigger", EventHubHandler,
		sdk.WithEventHubName("input-hub"),
		sdk.WithConnection("EventHubConnection"),
		sdk.WithConsumerGroup("watchtower-test"),
	)
	app.EventHubBatch("eventHubBatchTrigger", EventHubBatchHandler,
		sdk.WithEventHubName("input-hub-batch"),
		sdk.WithConnection("EventHubConnection"),
		sdk.WithConsumerGroup("watchtower-batch-test"),
	)

	worker.Start(app)
}
