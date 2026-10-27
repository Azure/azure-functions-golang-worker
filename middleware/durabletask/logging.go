package durabletask

import (
	"context"
	"log/slog"
)

// replayLogHandler binds only log delivery to the current Functions invocation.
// Upstream keeps the orchestrator's context replay-stable, so its logger cannot
// otherwise carry the invocation metadata and span. Do not expose this context
// to orchestrator code or store it on the shared runner.
type replayLogHandler struct {
	slog.Handler
	invocation context.Context
}

func (h *replayLogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return h.Handler.Enabled(h.invocation, level)
}

func (h *replayLogHandler) Handle(_ context.Context, record slog.Record) error {
	return h.Handler.Handle(h.invocation, record)
}

func (h *replayLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &replayLogHandler{Handler: h.Handler.WithAttrs(attrs), invocation: h.invocation}
}

func (h *replayLogHandler) WithGroup(name string) slog.Handler {
	return &replayLogHandler{Handler: h.Handler.WithGroup(name), invocation: h.invocation}
}
