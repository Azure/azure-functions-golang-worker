package main

import (
	"context"
	"log"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// BlobHandler handles blob trigger events.
// It receives the blob content as raw bytes — suitable for small to medium blobs.
// For large blobs, use the triggers/blob module which provides a *blob.Client
// for streaming access without loading the entire blob into memory.
func BlobHandler(ctx context.Context, data []byte) error {
	log.Printf("Blob Trigger Executed")
	log.Printf("Blob content length: %d bytes", len(data))

	if len(data) == 0 {
		log.Println("Blob is empty")
		return nil
	}

	// Log content for small blobs (< 1KB)
	if len(data) <= 1024 {
		log.Printf("Blob content: %s", string(data))
	} else {
		log.Printf("Blob content (first 100 bytes): %s...", string(data[:100]))
	}

	return nil
}

func main() {
	app := sdk.FunctionApp()

	app.Blob("blobTrigger", BlobHandler).
		Path("test-container/{name}").
		Connection("AzureWebJobsStorage")

	worker.Start(app)
}
