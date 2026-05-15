package log

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestBootstrapHandler_PrefixedLanguageWorkerConsoleLog(t *testing.T) {
	var buf bytes.Buffer
	h := NewBootstrap(&buf)
	logger := slog.New(h)

	logger.Info("starting up", "worker_id", "w-1")

	out := buf.String()
	if !strings.HasPrefix(out, "LanguageWorkerConsoleLog[") {
		t.Errorf("missing required prefix; got: %q", out)
	}
	if !strings.Contains(out, "[INFO] starting up") {
		t.Errorf("missing level + message segment; got: %q", out)
	}
	if !strings.Contains(out, "worker_id=w-1") {
		t.Errorf("missing attribute key=value; got: %q", out)
	}
}

func TestBootstrapHandler_LevelTags(t *testing.T) {
	cases := []struct {
		level slog.Level
		want  string
	}{
		{slog.LevelDebug, "DEBUG"},
		{slog.LevelInfo, "INFO"},
		{slog.LevelWarn, "WARN"},
		{slog.LevelError, "ERROR"},
	}
	for _, c := range cases {
		if got := levelTag(c.level); got != c.want {
			t.Errorf("levelTag(%v) = %q, want %q", c.level, got, c.want)
		}
	}
}

func TestBootstrapHandler_NilWriterFallsBackToStderr(t *testing.T) {
	// Smoke test: nil writer must not panic on construction.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("NewBootstrap(nil) panicked: %v", r)
		}
	}()
	h := NewBootstrap(nil)
	if h == nil {
		t.Fatal("expected non-nil handler from NewBootstrap(nil)")
	}
}
