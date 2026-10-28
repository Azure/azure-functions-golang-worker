package main

import (
	"context"
	"log/slog"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/azure/azure-functions-golang-worker/sdk"
	_ "github.com/azure/azure-functions-golang-worker/triggers/blob"
	"github.com/azure/azure-functions-golang-worker/worker"
)

func blobHandler(ctx context.Context, client *blob.Client) error {
	slog.InfoContext(ctx, "blob trigger executed", "url", client.URL())
	return nil
}

func main() {
	app := sdk.FunctionApp()
	app.Blob("blobTrigger", blobHandler,
		sdk.WithPath("test-container/{name}"),
		sdk.WithConnection("AzureWebJobsStorage"),
		sdk.WithSource("LogsAndContainerScan"),
	)
	worker.Start(app)
}
