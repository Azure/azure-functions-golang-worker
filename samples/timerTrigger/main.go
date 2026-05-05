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
	// Install the SDK's slog handler as the default. Every record emitted
	// from inside an invocation will carry invocation_id / function_name /
	// trigger_type alongside any user-supplied attributes.
	slog.SetDefault(sdk.NewLogger())

	app := sdk.FunctionApp()
	app.Timer("scheduledTask", TimerHandler,
		sdk.WithSchedule("*/10 * * * * *"),
	)
	worker.Start(app)
}
