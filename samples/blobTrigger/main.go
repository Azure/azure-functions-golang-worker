package main

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/azure/azure-functions-golang-worker/sdk"
	_ "github.com/azure/azure-functions-golang-worker/triggers/blob" // registers blob trigger client factory
	"github.com/azure/azure-functions-golang-worker/worker"
)

// BlobHandler handles blob trigger events using a *blob.Client.
// The client is scoped to the specific blob that triggered the function,
// supporting streaming access for blobs of any size.
func BlobHandler(ctx context.Context, client *blob.Client) error {
	log.Printf("Blob Trigger Executed for: %s", client.URL())

	// Download the blob content
	get, err := client.DownloadStream(ctx, nil)
	if err != nil {
		return fmt.Errorf("error downloading blob: %w", err)
	}

	data, err := io.ReadAll(get.Body)
	if err != nil {
		return fmt.Errorf("error reading blob body: %w", err)
	}
	get.Body.Close()

	log.Printf("Blob content length: %d bytes", len(data))
	if len(data) <= 1024 {
		log.Printf("Blob content: %s", string(data))
	} else {
		log.Printf("Blob content (first 100 bytes): %s...", string(data[:100]))
	}

	return nil
}

func main() {
	app := sdk.FunctionApp()

	app.Blob("blobTrigger", BlobHandler,
		sdk.WithPath("test-container/{name}"),
		sdk.WithConnection("AzureWebJobsStorage"),
	)

	worker.Start(app)
}
