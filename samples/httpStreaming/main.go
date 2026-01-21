package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// StreamHandler handles streaming HTTP responses
func StreamHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Starting streaming response...")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	for i := 0; i < 5; i++ {
		fmt.Fprintf(w, "data: event %d\n\n", i)
		flusher.Flush()
		time.Sleep(1 * time.Second)
	}

	log.Println("Streaming complete.")
}

func main() {
	app := sdk.FunctionApp()
	app.HTTP("stream", StreamHandler).
		Methods("GET").
		Auth("anonymous")
	worker.Start(app)
}
