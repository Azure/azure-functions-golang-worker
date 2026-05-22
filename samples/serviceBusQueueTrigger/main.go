package main

import (
	"context"
	"log/slog"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// ServiceBusQueueHandler handles messages from a Service Bus queue
func ServiceBusQueueHandler(ctx context.Context, msg bindings.ServiceBusMessage) error {
	slog.InfoContext(ctx, "servicebus queue trigger executed",
		"message_id", msg.MessageId,
		"body", string(msg.Body),
		"delivery_count", msg.DeliveryCount,
		"sequence_number", msg.SequenceNumber,
		"content_type", msg.ContentType,
		"session_id", msg.SessionId,
	)
	return nil
}

func main() {
	app := sdk.FunctionApp()

	app.ServiceBusQueue("queueFunc", ServiceBusQueueHandler,
		sdk.WithQueueName("input-queue"),
		sdk.WithConnection("ServiceBusConnection"),
	)

	worker.Start(app)
}
