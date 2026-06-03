package main

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/azure/azure-functions-golang-worker/middleware/otelfunc"
	"github.com/azure/azure-functions-golang-worker/otelcollector"
	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker"
)

func HelloHandler(w http.ResponseWriter, r *http.Request) {
	slog.InfoContext(r.Context(), "hello from collector-to-azure-monitor sample",
		slog.String("greeting", "world"))
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "hello from collector to azure monitor")
}

func main() {
	app := sdk.FunctionApp()

	// otelfunc bridges worker/user telemetry to the global OTel SDK, which
	// exports OTLP to OTEL_EXPORTER_OTLP_ENDPOINT (point it at the embedded
	// collector, e.g. http://localhost:4318). The Functions host reads the
	// same env var and sends its telemetry to the collector too.
	app.Use(otelfunc.Middleware())

	app.HTTP("hello", HelloHandler,
		sdk.WithMethods("GET"),
		sdk.WithAuth("anonymous"),
	)

	// otelcollector.WithCollector runs an embedded OTel Collector for the
	// lifetime of the worker: it starts (listening on localhost:4317/4318)
	// before serving and flushes/shuts down during teardown. With no options
	// it uses otel-collector-config.yaml next to the binary if present, else
	// the compiled-in Azure Monitor default.
	worker.Start(app, otelcollector.WithCollector())
}
