package function

import (
	"flag"
	"log"
	"net/url"

	"github.com/azure/azure-functions-golang-worker/internal"
)

func SetupWorker() {
	// Define CLI flags (similar to Dotnet approach).
	functionsURI := flag.String("functions-uri", "", "The host's gRPC endpoint URI (e.g. http://127.0.0.1:12345)")
	requestID := flag.String("functions-request-id", "", "The request ID passed by the host")
	workerID := flag.String("functions-worker-id", "", "The worker ID passed by the host")
	grpcMaxMessageLength := flag.Int("functions-grpc-max-message-length", 4*1024*1024, "Max gRPC message length")

	// If you still want to handle the port via an ENV var fallback, you can do so.
	// But let's just demonstrate flags for now.

	flag.Parse()

	// Validate required flags
	if *functionsURI == "" {
		log.Fatalf("missing required argument: --functions-uri")
	}
	if *requestID == "" {
		log.Fatalf("missing required argument: --functions-request-id")
	}
	if *workerID == "" {
		log.Fatalf("missing required argument: --functions-worker-id")
	}

	log.Printf("Parsed args: functions-uri=%s, functions-request-id=%s, functions-worker-id=%s, grpc-max-message-length=%d\n",
		*functionsURI, *requestID, *workerID, *grpcMaxMessageLength)

	// Optionally parse the URI to extract host/port (if it's well-formed).
	parsedURI, err := url.Parse(*functionsURI)
	if err != nil {
		log.Fatalf("invalid --functions-uri provided (%s): %v", *functionsURI, err)
	}

	// If the host is "127.0.0.1:12345", you might build an address like "127.0.0.1:12345"
	// For example:
	addr := parsedURI.Host
	if addr == "" {
		// fallback if no host was provided in the URI
		addr = ":8080"
	}

	// Now set up your worker as before:
	functionRegistry := internal.GetFunctionRegistry()
	dispatcher := internal.NewDispatcher(functionRegistry)

	log.Printf("Starting Go Azure Functions worker on %s...\n", addr)
	err = internal.StartWorkerServer(addr, dispatcher)
	if err != nil {
		log.Fatalf("Failed to start worker: %v", err)
	}
}
