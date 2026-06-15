package sdk

import (
	"context"
	"log/slog"
	"runtime/debug"
	"sync/atomic"
)

// This file provides helpers for safely running your own goroutines inside a
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
// [Go] and [Recover] close that gap. Wrap your background work with them and a
// panic in a goroutine becomes a logged error instead of a worker crash.

// Go runs fn in a new goroutine that is protected against panics.
//
// Use it instead of `go fn()` whenever you start background work from inside a
// function. If fn panics, Go recovers it, logs the error and stack trace
// (tagged with the current invocation so you can find it in Application
// Insights), and keeps your worker alive — instead of letting one panic crash
// every request running on it.
//
// Pass the handler's ctx so the logged error is linked to the right
// invocation. Note that Go does not cancel fn when ctx is done; if your
// goroutine should stop on cancellation, check ctx.Done() yourself.
//
// A nil fn does nothing.
//
//	func EventHubHandler(ctx context.Context, events []bindings.EventHubEvent) error {
//	    for _, e := range events {
//	        e := e
//	        sdk.Go(ctx, func() {
//	            // If this panics (say, a nil-pointer dereference), it's logged
//	            // with a full stack trace instead of crashing the worker.
//	            process(e)
//	        })
//	    }
//	    return nil
//	}
func Go(ctx context.Context, fn func()) {
	if fn == nil {
		return
	}
	go func() {
		defer Recover(ctx)
		fn()
	}()
}

// Recover is a panic guard for goroutines you start yourself.
//
// Reach for it when [Go] doesn't fit — for example when your goroutine also
// needs a `defer wg.Done()`, is part of a worker pool, or is a named function.
// Add it as the first line of the goroutine:
//
//	go func() {
//	    defer sdk.Recover(ctx)
//	    defer wg.Done()
//	    doBackgroundWork(ctx)
//	}()
//
// If the goroutine panics, Recover catches it, logs the error with a stack
// trace, and lets the goroutine return quietly so the worker stays up. If
// there's no panic, it does nothing, so it's always safe to defer.
//
// Recover intentionally stops the panic from propagating — that's how it keeps
// the worker alive. If your code needs to know the work failed, surface that
// from inside the goroutine (for example, send on a channel) before it panics.
func Recover(ctx context.Context) {
	if r := recover(); r != nil {
		goroutinePanicHandler()(ctx, r, debug.Stack())
	}
}

// GoroutinePanicHandler is called by [Go] and [Recover] whenever they recover
// a panic from a guarded goroutine. recovered is the value passed to panic and
// stack is the stack trace captured where the panic was caught.
//
// Most apps never need this — the default handler logs the panic for you. It's
// here for advanced cases where you want to report panics somewhere else (for
// example, an OpenTelemetry exception event or a metric). Your implementation
// must not panic and should return quickly.
type GoroutinePanicHandler func(ctx context.Context, recovered any, stack []byte)

// panicHandlerHolder is a swappable pointer to the active panic handler,
// mirroring the baseHolder pattern in log.go so the behavior can be
// overridden at runtime without changing call sites.
var panicHandlerHolder atomic.Pointer[GoroutinePanicHandler]

// SetGoroutinePanicHandler changes what happens when [Go] or [Recover] catch a
// panic. By default, panics are logged via slog with the recovered value and
// stack trace attached, tagged with the invocation so they're easy to find in
// Application Insights — which is all most apps need.
//
// Call this only if you want to report panics somewhere else as well (say, an
// OpenTelemetry exception event or a failure metric). Set it once at startup;
// it applies process-wide and is safe to call from any goroutine. Pass nil to
// go back to the default logging behavior.
func SetGoroutinePanicHandler(h GoroutinePanicHandler) {
	if h == nil {
		panicHandlerHolder.Store(nil)
		return
	}
	panicHandlerHolder.Store(&h)
}

// goroutinePanicHandler returns the active handler, falling back to the
// default slog-based reporter when none is installed.
func goroutinePanicHandler() GoroutinePanicHandler {
	if p := panicHandlerHolder.Load(); p != nil {
		return *p
	}
	return defaultGoroutinePanicHandler
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
