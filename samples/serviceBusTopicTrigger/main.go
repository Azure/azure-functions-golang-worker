package main

import (
	"context"
	"log/slog"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// ServiceBusTopicHandler handles messages from a Service Bus topic subscription
func ServiceBusTopicHandler(ctx context.Context, msg bindings.ServiceBusMessage) error {
	slog.InfoContext(ctx, "servicebus topic trigger executed",
		"message_id", msg.MessageId,
		"body", string(msg.Body),
		"delivery_count", msg.DeliveryCount,
		"sequence_number", msg.SequenceNumber,
		"subject", msg.Subject,
		"content_type", msg.ContentType,
		"test_id", msg.ApplicationProperties["testID"],
	)
	return nil
}

func main() {
	app := sdk.FunctionApp()

	app.ServiceBusTopic("topicFunc", ServiceBusTopicHandler,
		sdk.WithTopicName("orders"),
		sdk.WithSubscriptionName("processor"),
		sdk.WithConnection("ServiceBusConnection"),
	)

	worker.Start(app)
}
