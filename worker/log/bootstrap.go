package log

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

// bootstrapHandler is an slog.Handler used during the pre-gRPC phase of
// worker startup, before connectToHost has succeeded. It writes a
// human-readable line for each record to a writer (stderr in production)
// using the "LanguageWorkerConsoleLog[ts]" prefix the Functions host
// recognizes for stderr-routed worker logs:
//
//	LanguageWorkerConsoleLog[2026-05-07T15:04:05Z][INFO] worker starting up
//
// Once the gRPC stream is open and the system logger is wired, the
// worker swaps the slog default to one built around [NewSystem] and
// [NewUser] (backed by a [Writer]). The bootstrap handler is therefore
// short-lived and only handles a handful of records (config parsing,
// dial errors, etc.) before being replaced.
//
// The handler is concurrency-safe: a sync.Mutex serializes writes to the
// underlying writer to avoid interleaved lines.
type bootstrapHandler struct {
	mu sync.Mutex
	w  io.Writer
}

// NewBootstrap returns an slog.Handler that writes
// "LanguageWorkerConsoleLog[ts][level] message k=v ..." lines to w.
// Pass os.Stderr in production; tests can pass a *bytes.Buffer.
//
// Used both as the pre-gRPC default and as the [Writer]'s fallback when
// the gRPC stream returns send errors, so logs are never silently lost.
func NewBootstrap(w io.Writer) slog.Handler {
	if w == nil {
		w = os.Stderr
	}
	return &bootstrapHandler{w: w}
}

// Enabled reports whether the handler will process records at the given
// level. The bootstrap handler accepts everything from Debug up; the
// host's category filter takes effect only after the gRPC stream is open.
func (h *bootstrapHandler) Enabled(_ context.Context, _ slog.Level) bool {
	return true
}

func (h *bootstrapHandler) Handle(_ context.Context, r slog.Record) error {
	var sb strings.Builder
	ts := r.Time
	if ts.IsZero() {
		ts = time.Now()
	}
	fmt.Fprintf(&sb, "LanguageWorkerConsoleLog[%s][%s] %s",
		ts.UTC().Format(time.RFC3339), levelTag(r.Level), r.Message)
	r.Attrs(func(a slog.Attr) bool {
		fmt.Fprintf(&sb, " %s=%v", a.Key, a.Value.Any())
		return true
	})
	sb.WriteByte('\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := io.WriteString(h.w, sb.String())
	return err
}

func (h *bootstrapHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *bootstrapHandler) WithGroup(_ string) slog.Handler      { return h }

// levelTag converts an slog.Level to a short uppercase tag suitable for the
// bracketed level segment of a LanguageWorkerConsoleLog line.
func levelTag(l slog.Level) string {
	switch {
	case l <= slog.LevelDebug:
		return "DEBUG"
	case l <= slog.LevelInfo:
		return "INFO"
	case l <= slog.LevelWarn:
		return "WARN"
	default:
		return "ERROR"
	}
}
