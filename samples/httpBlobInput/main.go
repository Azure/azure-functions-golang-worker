package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/azure/azure-functions-golang-worker/sdk"
	_ "github.com/azure/azure-functions-golang-worker/sdk/extensions/blob"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// HTTPBlobInputHandler handles HTTP requests and reads a blob input using SDK Binding
func HTTPBlobInputHandler(w http.ResponseWriter, r *http.Request, blobClient *blob.Client) {
	log.Printf("Processing HTTP Trigger with Blob Input Client")

	// Use the injected client to read the blob
	get, err := blobClient.DownloadStream(context.Background(), nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("Error downloading blob: %v", err)))
		return
	}

	downloadedData, err := io.ReadAll(get.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("Error reading blob body: %v", err)))
		return
	}
	get.Body.Close()

	responseMsg := fmt.Sprintf("Read blob content via SDK: %s", string(downloadedData))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(responseMsg))
}

func main() {
	app := sdk.FunctionApp()

	app.HTTP("hello", HTTPBlobInputHandler).
		Methods("GET", "POST").
		Auth("anonymous").
		BlobInput("blobInput", "test-container/test.txt", "AzureWebJobsStorage")
		// Output binding removed as SDK client can be used for output

	worker.Start(app)
}
