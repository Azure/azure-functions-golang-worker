package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/azure/azure-functions-golang-worker/middleware/otelfunc"
	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker"
	"go.opentelemetry.io/collector/otelcol"
)

func HelloHandler(w http.ResponseWriter, r *http.Request) {
	slog.InfoContext(r.Context(), "hello from embedded collector sample",
		slog.String("greeting", "world"))
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "hello from embedded collector")
}

func main() {
	// Immediate diagnostic output to help debug crashes in Azure.
	fmt.Fprintln(os.Stderr, "embedded-collector-sample: main() entered")

	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "PANIC in embedded-collector-sample: %v\n", r)
			os.Exit(1)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	log.Println("embedded-collector-sample starting...")

	// Start the embedded OTel Collector.
	// It listens on localhost:4318 (OTLP/HTTP) and forwards to Azure Monitor
	// DCE endpoints using managed identity authentication.
	// Config is read from otel-collector-config.yaml in the app directory.
	var col *otelcol.Collector
	if os.Getenv("DISABLE_EMBEDDED_COLLECTOR") != "1" {
		var err error
		col, err = startCollector(ctx)
		if err != nil {
			log.Printf("WARNING: embedded collector failed to start: %v", err)
			log.Println("Continuing without collector - telemetry will not be forwarded to Azure Monitor")
		} else {
			// Give the collector a moment to bind its listener.
			time.Sleep(500 * time.Millisecond)
			log.Println("Embedded OTel Collector started on localhost:4318")
		}
	} else {
		log.Println("Embedded collector disabled via DISABLE_EMBEDDED_COLLECTOR=1")
	}

	// The function app's OTel SDK will read OTEL_EXPORTER_OTLP_ENDPOINT
	// (set to http://localhost:4318) and export to the embedded collector.
	// The host also reads the same env var and sends its telemetry here.
	app := sdk.FunctionApp()

	// No custom exporter needed — standard OTLP exporter via env vars.
	app.Use(otelfunc.Middleware())

	app.HTTP("hello", HelloHandler,
		sdk.WithMethods("GET"),
		sdk.WithAuth("anonymous"),
	)

	// worker.Start blocks until shutdown. When it returns, cancel the
	// collector context so it flushes and shuts down.
	worker.Start(app)
	cancel()

	// Give collector time to flush.
	_ = col
	time.Sleep(2 * time.Second)
}
