package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker"
)

// HelloHandler is intentionally minimal — the point of this sample is
// the middleware chain wrapping it, not the handler itself.
func HelloHandler(w http.ResponseWriter, r *http.Request) {
	slog.InfoContext(r.Context(), "handler running")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "hello from middleware sample")
}

// ──────────────────────────────────────────────────────────────────────────
// Style 1: function-based middleware via sdk.MiddlewareFunc.
//
// Use this shape when the middleware is stateless. Same pattern as
// net/http's HandlerFunc — wrap a `func(next Handler) Handler` in
// sdk.MiddlewareFunc to satisfy the sdk.Middleware interface.
// ──────────────────────────────────────────────────────────────────────────

// timingMiddleware logs the wall-clock duration of every invocation.
func timingMiddleware(next sdk.Handler) sdk.Handler {
	return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
		start := time.Now()
		err := next(ctx, mc)
		slog.InfoContext(ctx, "invocation finished",
			"duration_ms", time.Since(start).Milliseconds(),
			"function", mc.FunctionName, // field promotion from embedded *InvocationContext
			"err", err,
		)
		return err
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Style 2: struct-based middleware implementing sdk.Middleware.
//
// Use this shape when the middleware needs per-instance state (a
// configured client, a counter, a logger of its own, etc.). The struct
// implements the Wrap method directly; nothing else is required.
// ──────────────────────────────────────────────────────────────────────────

// invocationCounter tags every invocation with a monotonically
// increasing sequence number. The counter lives on the struct, so the
// state persists across invocations for the lifetime of the worker.
type invocationCounter struct {
	count atomic.Int64
}

// Wrap implements sdk.Middleware.
func (c *invocationCounter) Wrap(next sdk.Handler) sdk.Handler {
	return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
		n := c.count.Add(1)
		slog.InfoContext(ctx, "invocation counted",
			"invocation_n", n,
			"function", mc.FunctionName,
		)
		return next(ctx, mc)
	}
}

// ──────────────────────────────────────────────────────────────────────────
// Ordering demo: A, B, C registered in order.
//
// Registration order is the standard Go middleware convention (chi,
// gin, echo, net/http chain libraries): the first-registered middleware
// is the outermost — it runs first on the way in and last on the way
// out. For a request, the observable order is A.before → B.before →
// C.before → handler → C.after → B.after → A.after.
// ──────────────────────────────────────────────────────────────────────────

func orderingMiddleware(name string) sdk.Middleware {
	return sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
			slog.InfoContext(ctx, "before", "middleware", name)
			err := next(ctx, mc)
			slog.InfoContext(ctx, "after", "middleware", name)
			return err
		}
	})
}

func main() {
	app := sdk.FunctionApp()

	// Style 1: function-based, wrapped in sdk.MiddlewareFunc.
	app.Use(sdk.MiddlewareFunc(timingMiddleware))

	// Style 2: struct-based, registered directly.
	app.Use(&invocationCounter{})

	// Ordering demo. Each invocation logs A.before → B.before → C.before
	// → handler → C.after → B.after → A.after.
	app.Use(orderingMiddleware("A"))
	app.Use(orderingMiddleware("B"))
	app.Use(orderingMiddleware("C"))

	app.HTTP("hello", HelloHandler,
		sdk.WithMethods("GET"),
		sdk.WithAuth("anonymous"),
	)
	worker.Start(app)
}
