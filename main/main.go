package main

import (
	"log"
	"time"

	"github.com/azure/azure-functions-golang-worker/functions"
	"github.com/azure/azure-functions-golang-worker/sdk"
)

func CosmosDBTrigger(docs []sdk.CosmosDocument) {
	firstDoc := docs[0]
	log.Printf("Document ID: %s\n", firstDoc.ID)
	log.Printf("Document Data: %s\n", firstDoc.Data)
	log.Printf("Document Timestamp: %d\n", firstDoc.Timestamp)
}

func main() {
	// Create the app/handler
	app := functions.FunctionApp()

	// Register a CosmosDB trigger
	app.RegisterCosmosFunction(CosmosDBTrigger, "docs", "items", "test", "pythonworker37cdb_DOCUMENTDB")

	time.Sleep(240 * time.Second)
}
