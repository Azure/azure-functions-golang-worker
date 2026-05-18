package main

import (
	"log/slog"
	"net/http"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// HTTPTriggerHandler handles standard HTTP requests.
//
// The handler signature is the standard net/http one — no SDK-specific
// types appear in the user surface. Per-invocation metadata is reachable
// through r.Context() via sdk.FromContext.
//
// For an example of registering middleware (timing, tracing, etc.) around
// an HTTP handler, see samples/middleware.
func HTTPTriggerHandler(w http.ResponseWriter, r *http.Request) {
	ic, _ := sdk.FromContext(r.Context())

	slog.InfoContext(r.Context(), "processing http trigger",
		"path", r.URL.Path,
		"method", r.Method,
	)
	if ic != nil {
		slog.DebugContext(r.Context(), "invocation metadata",
			"trace_parent", ic.TraceContext.TraceParent,
		)
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Hello from Go Worker!"))
}

func main() {
	app := sdk.FunctionApp()

	app.HTTP("hello", HTTPTriggerHandler,
		sdk.WithMethods("GET", "POST"),
		sdk.WithAuth("anonymous"),
	)
	worker.Start(app)
}
