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

// func HttpTrigger(w http.ResponseWriter, r *http.Request) {
// 	w.Header().Set("Content-Type", "text/event-stream")
// 	w.Header().Set("Cache-Control", "no-cache")
// 	w.Header().Set("Connection", "keep-alive")

// 	// Flusher interface is required for streaming
// 	flusher, ok := w.(http.Flusher)
// 	if !ok {
// 		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
// 		return
// 	}

// 	// Simulate sensor data stream
// 	for i := 0; i < 10; i++ {
// 		temperature := 20 + i
// 		humidity := 50 + i

// 		// Write data to the stream
// 		fmt.Fprintf(w, "data: {\"temperature\": %d, \"humidity\": %d}\n\n", temperature, humidity)
// 		// Log data
// 		log.Printf("data: {\"temperature\": %d, \"humidity\": %d}\n\n", temperature, humidity)

// 		flusher.Flush()
// 		time.Sleep(1 * time.Second)
// 	}
// }

var cosmos = &functions.CosmosDBTrigger{
	ArgName:       "docs",
	ContainerName: "items",
	DatabaseName:  "test",
	Connection:    "pythonworker37cdb_DOCUMENTDB",
}

// var httpStruct = &functions.HttpTrigger{
// 	Route: "stream",
// }

func main() {
	// Create the app/handler
	app := functions.FunctionApp(functions.Anonymous)

	// Register Functions - customers will do this
	// app.RegisterFunction(HttpTrigger, httpStruct)
	app.RegisterFunction(CosmosDBTrigger, cosmos)

	// Start gRPC server
	for {
		time.Sleep((time.Second * 5))
	}
}
