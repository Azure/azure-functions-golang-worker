package main

import (
	"context"
	"io"
	"log"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/azure/azure-functions-golang-worker/sdk"
	_ "github.com/azure/azure-functions-golang-worker/sdk/extensions/blob"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// BlobHandler handles blob trigger events using SDK types
func BlobHandler(client *blob.Client) {
	log.Printf("Blob Trigger Executed for blob: %s", client.URL())

	// Download the blob content
	get, err := client.DownloadStream(context.Background(), nil)
	if err != nil {
		log.Printf("Error downloading blob: %v", err)
		return
	}

	downloadedData, err := io.ReadAll(get.Body)
	if err != nil {
		log.Printf("Error reading blob body: %v", err)
		return
	}
	get.Body.Close()

	log.Printf("Read content: %s", string(downloadedData))
}

func main() {
	app := sdk.FunctionApp()

	// Register with SDK binding type
	app.Blob("blobTrigger", BlobHandler).
		Path("test-container/test.txt").
		Connection("AzureWebJobsStorage")

	worker.Start(app)
}
