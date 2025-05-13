package main

import (
	"log"
	"time"

	"github.com/azure/azure-functions-golang-worker/functions"
)

func CosmosDBTrigger(docs []functions.CosmosDocument) {
	firstDoc := docs[0]
	log.Printf("Document ID: %s\n", firstDoc.ID)
	log.Printf("Document Data: %s\n", firstDoc.Data)
	log.Printf("Document Timestamp: %d\n", firstDoc.Timestamp)
}

func HttpTrigger() {

}

// var cosmos = &functions.CosmosDBTrigger{
// 	ArgName:       "docs",
// 	ContainerName: "items",
// 	DatabaseName:  "test",
// 	Connection:    "pythonworker37cdb_DOCUMENTDB",
// }

var http = &functions.HttpTrigger{
	Route: "stream",
}

func main() {
	// Create the app/handler
	app := functions.FunctionApp(functions.Anonymous)

	// Register a CosmosDB trigger
	// app.RegisterFunction(CosmosDBTrigger, cosmos)
	app.RegisterFunction(HttpTrigger, http)
	// app.RegisterCosmosFunction(CosmosDBTrigger, "docs", "items", "test", "pythonworker37cdb_DOCUMENTDB")

	for {
		time.Sleep((time.Second * 5))
	}
}
