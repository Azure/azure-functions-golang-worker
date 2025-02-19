package main

import (
	"github.com/azure/azure-functions-golang-worker/functions"
)

type Document struct {
	ID    string
	Items string
	Rid   string
	Etag  string
}

func cosmosDBFunction(doc Document) Document {
	return doc
}

func main() {
	// Create the app/handler
	app := functions.FunctionApp()

	// Register function(s)
	app.RegisterCosmosFunction(cosmosDBFunction)

	// Start the worker
	app.StartWorkerServer()
}
