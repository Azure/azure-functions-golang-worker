package worker

import (
	"log"

	"github.com/azure/azure-functions-golang-worker/sdk"
)

// Start initializes the worker and connects to the host.
func Start(app *sdk.App) {
	config, err := GetWorkerStartupConfig()
	if err != nil {
		log.Fatalf("Failed to parsing worker configuration: %v", err)
	}

	dispatcher := NewDispatcher(config, app)

	// Log that we are starting
	log.Printf("Starting Worker for Worker ID: %s", config.FunctionsWorkerId)

	client, err := connectToHost(config.FunctionsUri, config.FunctionsGrpcMaxMessageLength,
		config.FunctionsWorkerId)
	if err != nil {
		log.Fatalf("Error establishing connection to host's gRPC server: %v", err)
	}

	handleBidiStream(client, dispatcher)
}
