package main

import (
	"context"
	"log/slog"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	"github.com/azure/azure-functions-golang-worker/worker"
)

func TimerHandler(ctx context.Context, timer bindings.TimerInfo) error {
	// Pull invocation metadata off the context. ic.InvocationID, FunctionName,
	// TraceContext, RetryContext etc. are populated by the worker dispatcher.
	ic, _ := sdk.FromContext(ctx)

	// slog.InfoContext routes through the sdk log handler installed in main(),
	// which automatically attaches invocation_id, function_name, and
	// trigger_type to every record.
	slog.InfoContext(ctx, "timer trigger executed",
		"is_past_due", timer.IsPastDue,
		"last", timer.ScheduleStatus.Last,
		"next", timer.ScheduleStatus.Next,
	)

	if ic != nil && ic.RetryContext.RetryCount > 0 {
		slog.WarnContext(ctx, "running on retry",
			"retry_count", ic.RetryContext.RetryCount,
			"max_retry_count", ic.RetryContext.MaxRetryCount,
		)
	}
	return nil
}

func main() {
	// The SDK's slog handler is auto-installed as the default at package
	// init time, so every record emitted from inside an invocation
	// carries invocation_id / function_name / trigger_type alongside any
	// user-supplied attributes — no setup required. Call slog.SetDefault
	// yourself if you need full control over the slog backend.
	app := sdk.FunctionApp()
	app.Timer("scheduledTask", TimerHandler,
		sdk.WithSchedule("*/10 * * * * *"),
	)
	worker.Start(app)
}
