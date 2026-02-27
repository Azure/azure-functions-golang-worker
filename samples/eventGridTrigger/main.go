package main

import (
	"encoding/json"
	"log"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// EventGridHandler handles Event Grid trigger events.
func EventGridHandler(event bindings.EventGridEvent) {
	log.Printf("Event Grid trigger executed")
	log.Printf("Event ID: %s", event.Id)
	log.Printf("Event Type: %s", event.EventType)
	log.Printf("Subject: %s", event.Subject)
	log.Printf("Event Time: %s", event.EventTime)

	// Unmarshal the event data into a custom struct if needed
	var data map[string]interface{}
	if err := json.Unmarshal(event.Data, &data); err != nil {
		log.Printf("Error unmarshaling event data: %v", err)
		return
	}
	log.Printf("Event Data: %v", data)
}

func main() {
	app := sdk.FunctionApp()

	app.EventGrid("eventGridTrigger", EventGridHandler)

	worker.Start(app)
}
