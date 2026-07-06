package main

import (
	"context"
	"log/slog"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// CosmosDBTriggerHandler handles updates from CosmosDB.
func CosmosDBTriggerHandler(ctx context.Context, docs []bindings.CosmosDocument) error {
	for _, doc := range docs {
		slog.InfoContext(ctx, "cosmosdb document received",
			"document_id", doc.ID,
			"data", string(doc.Data),
		)
	}
	return nil
}

func main() {
	app := sdk.FunctionApp()
	app.CosmosDB("docs", CosmosDBTriggerHandler,
		sdk.WithDatabase("ToDoList"),
		sdk.WithContainer("Items"),
		sdk.WithConnection("CosmosDBConnection"),
		// Auto-provision the lease container on first run instead of
		// requiring it to exist beforehand.
		sdk.WithCreateLeaseContainerIfNotExists(true),
	)

	worker.Start(app)
}
