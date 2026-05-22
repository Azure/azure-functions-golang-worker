package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/azure/azure-functions-golang-worker/sdk"
	_ "github.com/azure/azure-functions-golang-worker/triggers/blob" // registers blob trigger client factory
	"github.com/azure/azure-functions-golang-worker/worker"
)

// BlobHandler handles blob trigger events using a *blob.Client.
// The client is scoped to the specific blob that triggered the function,
// supporting streaming access for blobs of any size.
//
// All log records are routed through the SDK's slog handler (auto-installed
// at package init), so each entry carries invocation_id, function_name,
// and trigger_type alongside the user-supplied attributes.
func BlobHandler(ctx context.Context, client *blob.Client) error {
	slog.InfoContext(ctx, "blob trigger executed", "url", client.URL())

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

	if len(data) <= 1024 {
		slog.InfoContext(ctx, "blob content read",
			"length", len(data),
			"content", string(data),
		)
	} else {
		slog.InfoContext(ctx, "blob content read",
			"length", len(data),
			"content_preview", string(data[:100]),
		)
	}

	return nil
}

func main() {
	app := sdk.FunctionApp()

	app.Blob("blobTrigger", BlobHandler,
		sdk.WithPath("test-container/{name}"),
		sdk.WithConnection("AzureWebJobsStorage"),
		sdk.WithSource("EventGrid"),
	)

	worker.Start(app)
}
