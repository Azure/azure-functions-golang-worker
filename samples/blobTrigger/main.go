package main

import (
	"context"
	"log"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// BlobHandler handles blob trigger events.
// In the triggers-only model, blob triggers receive the blob content as bytes.
// For SDK-type blob client access, use the triggers/blob module.
func BlobHandler(ctx context.Context, data []byte) error {
	log.Printf("Blob Trigger Executed")
	log.Printf("Blob content length: %d bytes", len(data))
	if len(data) > 0 && len(data) <= 1024 {
		log.Printf("Blob content: %s", string(data))
	}
	return nil
}

// BlobHandlerTyped is not used but shows how typed handler works
var _ sdk.EventGridHandler = func(ctx context.Context, event bindings.EventGridEvent) error {
	return nil
}

func main() {
	app := sdk.FunctionApp()

	// Note: For large blobs, consider using the triggers/blob module
	// which provides a *blob.Client for streaming access.
	// This sample uses raw bytes which works for small blobs.

	// TODO: Blob trigger with raw bytes needs a BlobDataHandler type
	// For now, we register using the generic RegisterFunction method
	// Full blob client support is in the triggers/blob module

	worker.Start(app)
}
