package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// HTTPTriggerHandler handles standard HTTP requests.
//
// The handler signature is the standard net/http one — no SDK-specific
// types appear in the user surface. Per-invocation metadata is reachable
// through r.Context() via sdk.FromContext.
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

// timingMiddleware is a tiny example of the sdk.Middleware shape: it
// times every invocation and emits a structured log record. Every Go
// developer who has used chi/gin/echo or net/http middleware should find
// this idiomatic.
//
// We declare it as a `func(next sdk.Handler) sdk.Handler` and adapt to the
// [sdk.Middleware] interface at registration time via [sdk.MiddlewareFunc],
// the same pattern as net/http's HandlerFunc. Middleware that needs to
// hold per-instance state would instead define its own struct type with a
// Wrap method.
func timingMiddleware(next sdk.Handler) sdk.Handler {
	return func(ctx context.Context, ic *sdk.InvocationContext) error {
		start := time.Now()
		err := next(ctx, ic)
		slog.InfoContext(ctx, "invocation finished",
			"duration_ms", time.Since(start).Milliseconds(),
			"err", err,
		)
		return err
	}
}

func main() {
	slog.SetDefault(sdk.NewLogger())

	app := sdk.FunctionApp()
	app.Use(sdk.MiddlewareFunc(timingMiddleware))

	app.HTTP("hello", HTTPTriggerHandler,
		sdk.WithMethods("GET", "POST"),
		sdk.WithAuth("anonymous"),
	)
	worker.Start(app)
}
