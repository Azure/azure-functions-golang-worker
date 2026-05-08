package main

import (
	"context"
	"log/slog"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// CosmosDBTriggerHandler handles updates from CosmosDB
func CosmosDBTriggerHandler(ctx context.Context, docs []bindings.CosmosDocument) error {
	if len(docs) > 0 {
		for _, doc := range docs {
			slog.InfoContext(ctx, "cosmosdb document received",
				"document_id", doc.ID,
				"data", string(doc.Data),
			)
		}
	} else {
		slog.InfoContext(ctx, "cosmosdb trigger fired with no documents")
	}
	return nil
}

func main() {
	app := sdk.FunctionApp()
	app.CosmosDB("docs", CosmosDBTriggerHandler,
		sdk.WithDatabase("ToDoList"),
		sdk.WithContainer("Items"),
		sdk.WithConnection("CosmosDBConnection"),
	)

	worker.Start(app)
}
