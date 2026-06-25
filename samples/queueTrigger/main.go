package main

import (
	"context"
	"log/slog"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// QueueHandler handles messages from an Azure Storage Queue.
func QueueHandler(ctx context.Context, msg bindings.QueueMessage) error {
	slog.InfoContext(ctx, "queue trigger executed",
		"id", msg.Id,
		"body", string(msg.Body),
		"dequeue_count", msg.DequeueCount,
		"pop_receipt", msg.PopReceipt,
		"expiration_time", msg.ExpirationTime,
		"insertion_time", msg.InsertionTime,
		"next_visible_time", msg.NextVisibleTime,
	)
	return nil
}

func main() {
	app := sdk.FunctionApp()

	app.Queue("queueFunc", QueueHandler,
		sdk.WithQueueName("myqueue"),
		sdk.WithConnection("AzureWebJobsStorage"),
	)

	worker.Start(app)
}
