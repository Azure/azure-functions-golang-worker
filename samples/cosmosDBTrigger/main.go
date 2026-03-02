package main

import (
	"log"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// CosmosDBTriggerHandler handles updates from CosmosDB
func CosmosDBTriggerHandler(docs []bindings.CosmosDocument) {
	if len(docs) > 0 {
		for _, doc := range docs {
			log.Printf("Document ID: %s, Data: %s\n", doc.ID, doc.Data)
		}
	} else {
		log.Println("No documents received")
	}
}

var retry = &sdk.RetryOptions{
	MaxRetryCount: 3,
	Strategy:      sdk.FixedDelay,
	DelayInterval: func() *time.Duration { d := 5 * time.Second; return &d }(),
}

func main() {
	app := sdk.FunctionApp()
	app.CosmosDB("docs", CosmosDBTriggerHandler).
		Database("ToDoList").
		Container("Items").
		Connection("CosmosDBConnection")

	worker.Start(app)
}
