package main

import (
	"log"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// ServiceBusTopicHandler handles messages from a Service Bus topic subscription
func ServiceBusTopicHandler(msg bindings.ServiceBusMessage) {
	log.Printf("Service Bus Topic Trigger Executed")
	log.Printf("Message ID: %s", msg.MessageId)
	log.Printf("Body: %s", string(msg.Body))
	log.Printf("Delivery Count: %d", msg.DeliveryCount)
	log.Printf("Sequence Number: %d", msg.SequenceNumber)

	if msg.Subject != "" {
		log.Printf("Subject: %s", msg.Subject)
	}
	if msg.ContentType != "" {
		log.Printf("Content Type: %s", msg.ContentType)
	}
}

func main() {
	app := sdk.FunctionApp()

	app.ServiceBusTopic("topicFunc", ServiceBusTopicHandler).
		TopicName("orders").
		SubscriptionName("processor").
		Connection("ServiceBusConnection")

	worker.Start(app)
}
