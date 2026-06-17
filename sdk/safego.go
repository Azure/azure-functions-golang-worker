package sdk

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
)

// This file provides a helper for safely running your own goroutines inside a
// function.
//
// Azure Functions already protects you from panics that happen directly in
// your handler: if your handler code panics, the worker catches it, fails just
// that one invocation, and reports the error (with the full stack) to the host
// and Application Insights. Your app keeps running.
//
// That protection does NOT cover goroutines you start yourself with a plain
// `go func() { ... }()`. In Go, if any goroutine panics and nothing recovers
// it, the ENTIRE process is torn down. In a Functions worker that means a
// single bad background goroutine crashes the worker, fails every other
// request currently running on it, and you don't even get a useful error —
// just "the worker exited." The actual cause never reaches your logs.
//
// [Recover] closes that gap. Defer it at the top of any goroutine you start
// and a panic becomes a logged error instead of a worker crash.

// Recover is a panic guard for goroutines you start yourself.
//
// It must be the first defer in the goroutine so it runs last and catches
// panics from all subsequent defers. Defers execute in LIFO order, so the
// first defer registered is the last to run — placing Recover first
// guarantees it is the final safety net:
//
//	go func() {
//	    defer sdk.Recover(ctx)   // ← first defer, runs last: catches everything
//	    defer wg.Done()
//	    doWork(ctx)
//	}()
//
// If the goroutine panics, Recover catches it, logs the error with a full
// stack trace (tagged with the current invocation so it's easy to find in
// Application Insights), and lets the goroutine exit cleanly so the worker
// stays up.
//
// Recover intentionally stops the panic from propagating. If the goroutine's
// failure should fail the invocation (trigger retries, surface in App Insights
// as an error), use [RecoverTo] instead — it captures the panic as an error
// via a pointer you supply:
//
//	g, ctx := errgroup.WithContext(ctx)
//	g.Go(func() (err error) {
//	    defer sdk.RecoverTo(ctx, &err)
//	    return process(event)
//	})
//	return g.Wait()
//
// For truly best-effort work where failure doesn't matter (cache warming,
// opportunistic telemetry), Recover alone is sufficient — it logs and
// swallows.
func Recover(ctx context.Context) {
	if r := recover(); r != nil {
		logRecoveredPanic(ctx, r, debug.Stack())
	}
}

// RecoverTo is a panic guard that captures the recovered panic as an error.
//
// Like [Recover], it must be the first defer so it runs last and catches
// panics from all subsequent defers.
//
// It is designed for goroutines whose failure should fail the enclosing
// invocation — triggering host retries and surfacing the error in Application
// Insights — without the boilerplate of a manual recover + named return:
//
//	g, ctx := errgroup.WithContext(ctx)
//	for _, e := range events {
//	    g.Go(func() (err error) {
//	        defer sdk.RecoverTo(ctx, &err)
//	        return process(e)
//	    })
//	}
//	return g.Wait() // non-nil on panic → invocation fails → host retries
//
// errp must be non-nil and should point to a named return value. If a panic
// occurs, RecoverTo logs the full stack trace (with invocation metadata) and
// sets *errp to a descriptive error. If *errp is already non-nil (i.e., the
// function returned an error normally before a deferred panic), the original
// error is preserved.
func RecoverTo(ctx context.Context, errp *error) {
	if r := recover(); r != nil {
		logRecoveredPanic(ctx, r, debug.Stack())
		if *errp == nil {
			*errp = fmt.Errorf("recovered panic: %v", r)
		}
	}
}

// logRecoveredPanic logs the recovered panic and its stack at error level.
// ctx flows through to slog so the sdk log handler can attach invocation
// metadata (invocation_id, function_name, trigger_type).
func logRecoveredPanic(ctx context.Context, recovered any, stack []byte) {
	slog.ErrorContext(ctx, "recovered panic in goroutine",
		slog.Any("panic", recovered),
		slog.String("stack", string(stack)),
	)
}
