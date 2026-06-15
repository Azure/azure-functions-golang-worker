package main

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// Product mirrors the rows of the dbo.Products table being monitored.
type Product struct {
	ProductID int    `json:"ProductId"`
	Name      string `json:"Name"`
	Cost      int    `json:"Cost"`
}

// ProductsChanged is invoked with a batch of row changes captured from
// dbo.Products via SQL Change Tracking. Each SQLChange carries an Operation
// (Insert / Update / Delete) and the row payload as raw JSON.
func ProductsChanged(ctx context.Context, changes []bindings.SQLChange) error {
	if len(changes) == 0 {
		slog.InfoContext(ctx, "sql trigger fired with empty batch")
		return nil
	}
	for _, change := range changes {
		var p Product
		if err := json.Unmarshal(change.Item, &p); err != nil {
			slog.ErrorContext(ctx, "failed to decode SQL row",
				"operation", change.Operation.String(),
				"error", err.Error(),
			)
			continue
		}
		slog.InfoContext(ctx, "sql row change",
			"operation", change.Operation.String(),
			"product_id", p.ProductID,
			"name", p.Name,
			"cost", p.Cost,
		)
	}
	return nil
}

func main() {
	app := sdk.FunctionApp()
	app.SQL("productsChanged", ProductsChanged,
		sdk.WithTable("dbo.Products"),
		sdk.WithConnection("AzureWebJobsSqlConnectionString"),
	)
	worker.Start(app)
}
