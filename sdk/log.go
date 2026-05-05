package sdk

import (
	"context"
	"log/slog"
	"os"
)

// NewLogHandler returns a slog.Handler that automatically attaches the
// current invocation's metadata (invocation_id, function_name, trigger_type)
// as attributes to every record processed inside an invocation context.
//
// If base is nil, a JSON handler writing to stderr with no extra options is
// used — sufficient for most Functions deployments where the host captures
// stdout/stderr and forwards records to Application Insights / OTLP / Log
// Analytics.
//
// The wrapper plays well with [slog.Logger.With], [slog.Logger.WithGroup],
// and slog's structured-attribute APIs: those operations delegate to the
// base handler so user attributes are preserved alongside the
// invocation-scoped attributes injected by NewLogHandler.
//
// Inspired by aws-lambda-go's lambdacontext.NewLogHandler — the design
// pattern is "wrap the user's slog.Handler, look up invocation metadata via
// FromContext, and add it as record attributes."
//
// Example:
//
//	slog.SetDefault(slog.New(sdk.NewLogHandler(nil)))
//
//	func TimerHandler(ctx context.Context, t bindings.TimerInfo) error {
//	    slog.InfoContext(ctx, "timer fired", "schedule", t.ScheduleStatus)
//	    // record now carries invocation_id, function_name, trigger_type
//	    return nil
//	}
func NewLogHandler(base slog.Handler) slog.Handler {
	if base == nil {
		base = slog.NewJSONHandler(os.Stderr, nil)
	}
	return &invocationLogHandler{base: base}
}

// NewLogger is shorthand for slog.New(NewLogHandler(nil)).
//
// Use it as the default logger when no custom slog handler is required:
//
//	slog.SetDefault(sdk.NewLogger())
func NewLogger() *slog.Logger {
	return slog.New(NewLogHandler(nil))
}

// invocationLogHandler is the slog.Handler implementation returned by
// NewLogHandler. It is intentionally trivial: it forwards every method to
// the wrapped base handler, only mutating the record in Handle to add
// invocation-scoped attributes when the context carries an InvocationContext.
type invocationLogHandler struct {
	base slog.Handler
}

func (h *invocationLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.base.Enabled(ctx, level)
}

func (h *invocationLogHandler) Handle(ctx context.Context, r slog.Record) error {
	if ic, ok := FromContext(ctx); ok && ic != nil {
		if ic.InvocationID != "" {
			r.AddAttrs(slog.String("invocation_id", ic.InvocationID))
		}
		if ic.FunctionName != "" {
			r.AddAttrs(slog.String("function_name", ic.FunctionName))
		}
		if ic.TriggerType != "" {
			r.AddAttrs(slog.String("trigger_type", ic.TriggerType))
		}
	}
	return h.base.Handle(ctx, r)
}

func (h *invocationLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &invocationLogHandler{base: h.base.WithAttrs(attrs)}
}

func (h *invocationLogHandler) WithGroup(name string) slog.Handler {
	return &invocationLogHandler{base: h.base.WithGroup(name)}
}
