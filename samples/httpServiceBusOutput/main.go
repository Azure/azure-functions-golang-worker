package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// HTTPServiceBusHandler handles HTTP requests and sends messages to Service Bus queue and topic
func HTTPServiceBusHandler(w http.ResponseWriter, r *http.Request, queueMsg *string, topicMsg *string) {
	log.Printf("HTTP Trigger received — sending messages to Service Bus")

	body := fmt.Sprintf("Hello from HTTP trigger! Method: %s, URL: %s", r.Method, r.URL.String())

	*queueMsg = body
	*topicMsg = body

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Messages sent to Service Bus queue and topic!"))
}

func main() {
	app := sdk.FunctionApp()

	app.HTTP("send", HTTPServiceBusHandler).
		Methods("GET", "POST").
		Auth("anonymous").
		ServiceBusQueueOutput("queueMsg", "input-queue", "ServiceBusConnection").
		ServiceBusTopicOutput("topicMsg", "orders", "ServiceBusConnection")

	worker.Start(app)
}
