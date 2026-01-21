package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// HTTPBlobInputHandler handles HTTP requests and reads a blob input
// The blob content is passed as the third argument (blobData).
func HTTPBlobInputHandler(w http.ResponseWriter, r *http.Request, blobContent []byte) {
	log.Printf("Processing HTTP Trigger with Blob Input")

	if len(blobContent) == 0 {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("Blob content is empty or not found"))
		return
	}

	responseMsg := fmt.Sprintf("Read blob content: %s", string(blobContent))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(responseMsg))
}

func main() {
	app := sdk.FunctionApp()

	app.HTTP("hello", HTTPBlobInputHandler).
		Methods("GET", "POST").
		Auth("anonymous").
		BlobInput("blobInput", "test-container/test.txt", "AzureWebJobsStorage").
		BlobOutput("blobOutput", "output-container/output.txt", "AzureWebJobsStorage")

	worker.Start(app)
}
