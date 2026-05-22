package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// StreamHandler handles streaming HTTP responses. Use slog.*Context
// (not stdlib log.Println) so each record carries trace.id / span.id
// and the per-invocation invocation_id / function_name / trigger_type
// attrs the SDK adds via FromContext. Without ctx, log records reach
// the host as opaque stderr text -- no correlation, no structured
// attrs. See README.md observability section.
func StreamHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	slog.InfoContext(ctx, "streaming response starting")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	for i := 0; i < 5; i++ {
		fmt.Fprintf(w, "data: event %d\n\n", i)
		flusher.Flush()
		time.Sleep(1 * time.Second)
	}

	slog.InfoContext(ctx, "streaming response complete", "events_sent", 5)
}

func main() {
	app := sdk.FunctionApp()
	app.HTTP("stream", StreamHandler,
		sdk.WithMethods("GET"),
		sdk.WithAuth("anonymous"),
	)
	worker.Start(app)
}
