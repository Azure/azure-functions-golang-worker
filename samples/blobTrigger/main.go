package main

import (
	"log"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// BlobHandler handles blob trigger events
func BlobHandler(data []byte, outputBlob *[]byte) {
	log.Printf("Blob Trigger Executed! Blob content size: %d", len(data))

	// Copy the blob content to the output blob
	*outputBlob = data
	log.Printf("Written to output blob")
}

func main() {
	app := sdk.FunctionApp()

	app.Blob("blobTrigger", BlobHandler).
		Path("test-container/test.txt").
		Connection("AzureWebJobsStorage").
		BlobOutput("outputBlob", "test-container/test1.txt", "AzureWebJobsStorage")

	worker.Start(app)
}
