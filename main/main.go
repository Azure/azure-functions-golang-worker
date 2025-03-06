package main

import (
	"log"
	"time"

	"github.com/azure/azure-functions-golang-worker/functions"
	"github.com/azure/azure-functions-golang-worker/sdk"
)

func CosmosDBTrigger(doc []sdk.CosmosDocument) {
	firstDoc := doc[0]
	log.Printf("Document ID: %s\n", firstDoc.ID)
	log.Printf("Document Data: %s\n", firstDoc.Data)
	log.Printf("Document Timestamp: %d\n", firstDoc.Timestamp)
}

func main() {
	// Create the app/handler
	app := functions.FunctionApp()

	// Register a CosmosDB trigger
	app.RegisterCosmosFunction(CosmosDBTrigger)

	time.Sleep(240 * time.Second)
}
