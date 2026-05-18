package main

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/azure/azure-functions-golang-worker/middleware/otelfunc"
	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// HelloHandler is intentionally minimal — the point of this sample is
// `app.Use(otelfunc.Middleware())` in main(). Every invocation emits an
// OpenTelemetry span (named `function hello`) and every slog record
// inside the handler carries trace.id / span.id so logs and traces
// correlate end-to-end in the configured OTel backend.
func HelloHandler(w http.ResponseWriter, r *http.Request) {
	slog.InfoContext(r.Context(), "hello from otel sample")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "hello")
}

func main() {
	app := sdk.FunctionApp()

	// Zero-config: otelfunc.Middleware reads the standard OTEL_*
	// environment variables and auto-configures both a TracerProvider
	// and a LoggerProvider against the OTLP HTTP endpoint. See
	// local.settings.json for the variables to set.
	app.Use(otelfunc.Middleware())

	app.HTTP("hello", HelloHandler,
		sdk.WithMethods("GET"),
		sdk.WithAuth("anonymous"),
	)
	worker.Start(app)
}
