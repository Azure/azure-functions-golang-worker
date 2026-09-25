package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"

	"github.com/azure/azure-functions-golang-worker/samples/genericTriggers/queue"
	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

type Order struct {
	ID       string `json:"id"`
	Quantity int    `json:"quantity"`
}

func main() {
	scenario := os.Getenv("GENERIC_SCENARIO")
	if scenario == "" {
		scenario = "queues"
	}
	app, err := newApp(scenario)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	// Inspect the exact host metadata without starting Core Tools or a listener.
	if len(os.Args) == 2 && os.Args[1] == "--metadata" {
		var functions []*sdk.RegisteredFunction
		app.GetRegisteredFunctions().Range(func(_, value any) bool {
			functions = append(functions, value.(*sdk.RegisteredFunction))
			return true
		})
		sort.Slice(functions, func(i, j int) bool { return functions[i].FuncName < functions[j].FuncName })
		for _, fn := range functions {
			data, err := json.MarshalIndent(struct {
				Name     string             `json:"name"`
				Bindings []bindings.Binding `json:"bindings"`
			}{fn.FuncName, fn.RawBindings}, "", "  ")
			if err != nil {
				slog.Error(err.Error())
				os.Exit(1)
			}
			fmt.Println(string(data))
		}
		return
	}
	worker.Start(app)
}

func newApp(scenario string) (*sdk.App, error) {
	app := sdk.FunctionApp()
	app.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
			slog.InfoContext(ctx, "generic middleware", "function", mc.FunctionName, "trigger", mc.TriggerType)
			return next(ctx, mc)
		}
	}))
	switch scenario {
	case "queues":
		// B: raw binary or text arrives unchanged; JSON is not unquoted.
		app.GenericTrigger("RawQueue", rawOrder, queueTrigger("%GenericRawQueue%"))
		// B: use the same registration with a JSON model instead of []byte.
		app.GenericTrigger("JSONQueue", handleOrder, queueTrigger("%GenericJSONQueue%"))
		// C: typed package translates configuration and passes the SAME handler.
		queue.Register(app, "TypedQueue", handleOrder, queue.Config{
			QueueName: "%GenericTypedQueue%", Connection: "AzureWebJobsStorage",
		})
		// Metadata is available through the normal invocation context.
		app.GenericTrigger("MetadataQueue", metadataOrder, queueTrigger("%GenericMetadataQueue%"))
	case "eventhub":
		// A JSON array in one event and a host batch are distinct concepts.
		// Request a host batch explicitly. The extension still controls batching.
		app.GenericTrigger("BatchOrders", batchOrders, &bindings.GenericTrigger{
			Type: "eventHubTrigger", Name: "events", DataType: "binary", Cardinality: "many",
			Properties: map[string]any{
				"eventHubName": "%OrdersEventHub%", "connection": "OrdersEventHubConnection", "consumerGroup": "$Default",
			},
		})
	case "mcp":
		// No mcpToolTrigger type or dispatch case is added to the core SDK.
		// The MCP host extension consumes this trigger's return value directly.
		app.GenericTrigger("EchoTool", echoTool, &bindings.GenericTrigger{
			Type: "mcpToolTrigger", Name: "call", DataType: "string",
			Properties: map[string]any{
				"toolName": "echo", "description": "Echo text using a generic Go trigger",
				"toolProperties": `[{"propertyName":"text","propertyType":"string","description":"Text to echo","isRequired":true}]`,
			},
		})
	default:
		return nil, fmt.Errorf("unknown GENERIC_SCENARIO %q; choose queues, eventhub, or mcp", scenario)
	}
	return app, nil
}

func queueTrigger(name string) *bindings.GenericTrigger {
	return &bindings.GenericTrigger{
		Type: "queueTrigger", Name: "message", DataType: "string",
		Properties: map[string]any{"queueName": name, "connection": "AzureWebJobsStorage"},
	}
}

func rawOrder(ctx context.Context, body []byte) error {
	slog.InfoContext(ctx, "raw order", "body", string(body))
	return nil
}

func handleOrder(ctx context.Context, order Order) error {
	if order.ID == "" {
		return fmt.Errorf("order id is required")
	}
	slog.InfoContext(ctx, "typed order", "id", order.ID, "quantity", order.Quantity)
	return nil
}

func metadataOrder(ctx context.Context, body []byte) error {
	ic, ok := sdk.FromContext(ctx)
	if !ok {
		return fmt.Errorf("missing invocation context")
	}
	metadata, err := json.Marshal(ic.TriggerMetadataValues)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "order metadata", "body", string(body), "metadata", string(metadata))
	return nil
}

func batchOrders(ctx context.Context, orders []Order) error {
	for _, order := range orders {
		if err := handleOrder(ctx, order); err != nil {
			return err
		}
	}
	return nil
}

type ToolCall struct {
	Arguments struct {
		Text string `json:"text"`
	} `json:"arguments"`
}

func echoTool(ctx context.Context, call ToolCall) (string, error) {
	slog.InfoContext(ctx, "echo tool", "text", call.Arguments.Text)
	return call.Arguments.Text, nil
}
