package main

import (
	"time"

	"github.com/azure/azure-functions-golang-worker/functions"
)

// type Document struct {
// 	ID    string
// 	Items string
// 	Rid   string
// 	Etag  string
// }

// func cosmosDBFunction(doc Document) Document {
// 	return doc
// }

func main() {
	// Create the app/handler
	_ = functions.FunctionApp()

	time.Sleep(120 * time.Second)

	// Register function(s)
	// app.RegisterCosmosFunction(cosmosDBFunction)
	// app.RegisterCosmosFunction(cosmosDBFunction, connectionStringToCosmos)
	// app.RegisterHttpFunction(httpFunction)
	// app.RegisterBlobFunction(blobFunction)
	// app.RegisterQueueFunction(queueFunction)
}
