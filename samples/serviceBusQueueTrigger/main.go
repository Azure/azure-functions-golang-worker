package main

import (
	"log"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// ServiceBusQueueHandler handles messages from a Service Bus queue
func ServiceBusQueueHandler(msg bindings.ServiceBusMessage) {
	log.Printf("Service Bus Queue Trigger Executed")
	log.Printf("Message ID: %s", msg.MessageId)
	log.Printf("Body: %s", string(msg.Body))
	log.Printf("Delivery Count: %d", msg.DeliveryCount)
	log.Printf("Sequence Number: %d", msg.SequenceNumber)

	if msg.ContentType != "" {
		log.Printf("Content Type: %s", msg.ContentType)
	}
	if msg.SessionId != "" {
		log.Printf("Session ID: %s", msg.SessionId)
	}
}

func main() {
	app := sdk.FunctionApp()

	app.ServiceBusQueue("queueFunc", ServiceBusQueueHandler).
		QueueName("input-queue").
		Connection("ServiceBusConnection")

	worker.Start(app)
}
