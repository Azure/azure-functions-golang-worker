package sdk

import (
	"context"
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
// Add it as the first defer in any goroutine launched from a handler:
//
//	go func() {
//	    defer sdk.Recover(ctx)
//	    defer wg.Done()
//	    doWork(ctx)
//	}()
//
// If the goroutine panics, Recover catches it, logs the error with a full
// stack trace (tagged with the current invocation so it's easy to find in
// Application Insights), and lets the goroutine exit cleanly so the worker
// stays up.
//
// Recover intentionally stops the panic from propagating. To surface the
// failure to the caller, use a named return and convert the panic to an error:
//
//	g, ctx := errgroup.WithContext(ctx)
//	for _, e := range events {
//	    g.Go(func() (err error) {
//	        defer func() {
//	            if r := recover(); r != nil {
//	                slog.ErrorContext(ctx, "panic processing event",
//	                    slog.Any("panic", r),
//	                    slog.String("stack", string(debug.Stack())))
//	                err = fmt.Errorf("panic: %v", r)
//	            }
//	        }()
//	        return process(e)
//	    })
//	}
//	return g.Wait()
//
// The named-return pattern gives you both: the panic is logged with invocation
// metadata (via slog + the SDK log handler), and it propagates as an error so
// the invocation fails and the host can retry.
//
// For truly best-effort work where failure doesn't matter (cache warming,
// opportunistic telemetry), Recover alone is sufficient — it logs and
// swallows.
func Recover(ctx context.Context) {
	if r := recover(); r != nil {
		defaultGoroutinePanicHandler(ctx, r, debug.Stack())
	}
}

// defaultGoroutinePanicHandler logs the recovered panic and its stack at error
// level. ctx flows through to slog so the sdk log handler can attach
// invocation metadata (invocation_id, function_name, trigger_type).
func defaultGoroutinePanicHandler(ctx context.Context, recovered any, stack []byte) {
	slog.ErrorContext(ctx, "recovered panic in goroutine",
		slog.Any("panic", recovered),
		slog.String("stack", string(stack)),
	)
}
