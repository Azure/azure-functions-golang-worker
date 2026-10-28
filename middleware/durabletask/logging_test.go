package durabletask

import (
	"context"
	"errors"
	"log/slog"
	"testing"
)

type contextCheckingLogHandler struct {
	slog.Handler
	expected context.Context
	handled  bool
	err      error
}

func (h *contextCheckingLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return ctx == h.expected && level >= slog.LevelWarn
}

func (h *contextCheckingLogHandler) Handle(ctx context.Context, _ slog.Record) error {
	if ctx != h.expected {
		return errors.New("wrong log context")
	}
	h.handled = true
	return h.err
}

func TestReplayLogHandler_RespectsCustomHandler(t *testing.T) {
	invocation, cancel := context.WithCancel(context.Background())
	defer cancel()
	wantErr := errors.New("log sink unavailable")
	base := &contextCheckingLogHandler{expected: invocation, err: wantErr}
	handler := &replayLogHandler{Handler: base, invocation: invocation}
	logger := slog.New(handler)
	logger.Info("filtered")
	if base.handled {
		t.Fatal("custom log level filter was bypassed")
	}
	logger.Warn("delivered")
	if !base.handled {
		t.Fatal("Enabled did not receive invocation context")
	}
	if err := handler.Handle(context.Background(), slog.Record{}); !errors.Is(err, wantErr) {
		t.Fatalf("handler error = %v, want %v", err, wantErr)
	}
}
