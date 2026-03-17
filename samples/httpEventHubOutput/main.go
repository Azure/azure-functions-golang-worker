package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/azure/azure-functions-golang-worker/sdk"
	_ "github.com/azure/azure-functions-golang-worker/sdk/extensions/eventhub"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// HTTPEventHubHandler handles HTTP requests and sends events to an EventHub
func HTTPEventHubHandler(w http.ResponseWriter, r *http.Request, producer *azeventhubs.ProducerClient) {
	log.Printf("HTTP Trigger received — sending event to EventHub")

	batch, err := producer.NewEventDataBatch(context.Background(), nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("Failed to create event batch: %v", err)))
		return
	}

	msg := fmt.Sprintf(`{"message": "Hello from HTTP trigger!", "method": "%s", "url": "%s"}`, r.Method, r.URL.String())
	err = batch.AddEventData(&azeventhubs.EventData{
		Body: []byte(msg),
	}, nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("Failed to add event: %v", err)))
		return
	}

	err = producer.SendEventDataBatch(context.Background(), batch, nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("Failed to send event batch: %v", err)))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Event sent to EventHub successfully!"))
}

func main() {
	app := sdk.FunctionApp()

	app.HTTP("send", HTTPEventHubHandler).
		Methods("GET", "POST").
		Auth("anonymous").
		EventHubOutput("producer", "myeventhub", "EventHubConnection")

	worker.Start(app)
}
