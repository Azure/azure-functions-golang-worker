package main

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// EventGridHandler handles Event Grid trigger events.
func EventGridHandler(ctx context.Context, event bindings.EventGridEvent) error {
	slog.InfoContext(ctx, "eventgrid trigger executed",
		"event_id", event.Id,
		"event_type", event.EventType,
		"subject", event.Subject,
		"event_time", event.EventTime,
	)

	// Unmarshal the event data into a custom struct if needed
	var data map[string]interface{}
	if err := json.Unmarshal(event.Data, &data); err != nil {
		slog.ErrorContext(ctx, "unmarshal event data failed", "err", err)
		return err
	}
	slog.InfoContext(ctx, "event data", "data", data)
	return nil
}

func main() {
	app := sdk.FunctionApp()

	app.EventGrid("eventGridTrigger", EventGridHandler)

	worker.Start(app)
}
