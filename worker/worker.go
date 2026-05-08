package worker

import (
	"log"

	"github.com/azure/azure-functions-golang-worker/sdk"
)

// Start initializes the worker and connects to the host.
func Start(app *sdk.App) {
	config, err := GetWorkerStartupConfig()
	if err != nil {
		log.Fatalf("Failed to parse worker configuration: %v", err)
	}

	dispatcher := NewDispatcher(config, app)

	// Start the in-process HTTP proxy (used for HTTP streaming via the
	// "HttpUri" capability). Returns nil if the app has no HTTP triggers
	// or the loopback listener can't be opened — in either case the worker
	// falls back to the gRPC-buffered HTTP path.
	dispatcher.HTTPProxy = startHTTPProxy(app)

	// Log that we are starting
	log.Printf("Starting Worker for Worker ID: %s", config.FunctionsWorkerId)

	client, err := connectToHost(config.FunctionsUri, config.FunctionsGrpcMaxMessageLength,
		config.FunctionsWorkerId)
	if err != nil {
		log.Fatalf("Error establishing connection to host's gRPC server: %v", err)
	}

	handleBidiStream(client, dispatcher)
}
