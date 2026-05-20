package log

import (
	"context"
	"log/slog"
	"sync/atomic"
)

// Observer is invoked for every user log record after the RpcLog
// has been emitted on the gRPC stream. Observers are an opt-in extension
// point: the worker itself never imports any observability backend, so
// users who don't register one pay zero binary-size cost for that path.
//
// The typical caller is the otelfunc middleware, which registers an
// otelslog-bridge observer so user log records also flow through the
// configured OpenTelemetry LoggerProvider. Observer errors are swallowed
// silently -- the RpcLog has already gone out, so a failing observer
// must not derail the user's invocation.
type Observer func(ctx context.Context, record slog.Record)

// observers is the package-global slice of registered observers.
// Reads happen on the user-log emit hot path so we keep this as an
// atomic.Pointer to a (read-only) slice; writes copy-on-write.
var observers atomic.Pointer[[]Observer]

// RegisterObserver appends fn to the set of observers invoked for
// every user slog record. Safe to call concurrently. Idempotency is the
// caller's responsibility: registering the same function twice will
// invoke it twice per record.
//
// Observers are called synchronously after the RpcLog has been enqueued
// on the outbound gRPC stream. They run in the goroutine that emitted
// the record, so a slow observer back-pressures the user handler. The
// otelfunc middleware avoids this by using the BatchProcessor on its
// LoggerProvider; the bridge call itself just enqueues the record.
func RegisterObserver(fn Observer) {
	if fn == nil {
		return
	}
	for {
		cur := observers.Load()
		var next []Observer
		if cur != nil {
			next = make([]Observer, len(*cur), len(*cur)+1)
			copy(next, *cur)
		} else {
			next = make([]Observer, 0, 1)
		}
		next = append(next, fn)
		if observers.CompareAndSwap(cur, &next) {
			return
		}
	}
}
