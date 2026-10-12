package sdk_test

import (
	"context"
	"fmt"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
)

// No user-authored binding type, registration package, or SDK client is needed.
func ExampleApp_GenericTrigger() {
	app := sdk.FunctionApp()
	registered := app.GenericTrigger("RawOrder", func(ctx context.Context, body []byte) error {
		fmt.Println(string(body))
		return nil
	}, &bindings.GenericTrigger{
		Type: "queueTrigger", Name: "message", DataType: "string",
		Properties: map[string]any{
			"queueName": "orders", "connection": "AzureWebJobsStorage",
		},
	})
	fmt.Println(registered.TriggerType)
	// Start the application with worker.Start(app).
	// Output: queueTrigger
}

func ExampleApp_GenericTrigger_json() {
	type Order struct {
		ID string `json:"id"`
	}
	app := sdk.FunctionApp()
	registered := app.GenericTrigger("JSONOrder", func(ctx context.Context, order Order) error {
		fmt.Println(order.ID)
		return nil
	}, &bindings.GenericTrigger{
		Type: "queueTrigger", Name: "message",
		Properties: map[string]any{
			"queueName": "orders", "connection": "AzureWebJobsStorage",
		},
	})
	fmt.Println(registered.FuncName)
	// Output: JSONOrder
}
