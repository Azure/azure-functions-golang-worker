package main

import (
	"log"
	"time"

	"github.com/azure/azure-functions-golang-worker/functions"
)

var retryCounter int32 = 0

const failUntil = 2

func CosmosDBTrigger(docs []functions.CosmosDocument) {
	if retryCounter < failUntil {
		retryCounter++
		log.Printf("Retrying CosmosDBTrigger, attempt %d/%d\n", retryCounter, failUntil)
		panic("Simulated failure for CosmosDBTrigger")
	}
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

var cosmos = &functions.CosmosDB{
	ArgName:       "docs",
	DatabaseName:  "test",
	ContainerName: "items",
	Connection:    "pythonworker37cdb_DOCUMENTDB",
}

var delayInterval = 3 * time.Second
var retry = &functions.RetryOptions{
	MaxRetryCount: 5,
	DelayInterval: &delayInterval,
	Strategy:      functions.FixedDelay,
}

// var httpStruct = &functions.HttpTrigger{
// 	Route: "stream",
// }

func main() {
	// Create the app/handler
	app := functions.FunctionApp()

	// Register the functions that should be used
	app.
		RegisterFunction(CosmosDBTrigger, cosmos).
		WithRetry(retry)

	// Start the gRPC server
	app.Start()

	// Register Functions - customers will do this
	// app.RegisterFunction(HttpTrigger, httpStruct)
	// app.RegisterFunction(CosmosDBTrigger, cosmos)
}
