package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestNewLogHandler_AttachesInvocationAttrs(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(NewLogHandler(base))

	ic := &InvocationContext{
		InvocationID: "inv-42",
		FunctionName: "Hello",
		TriggerType:  "httpTrigger",
	}
	ctx := NewContext(context.Background(), ic)

	logger.InfoContext(ctx, "ping")

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("log output is not valid JSON: %v\n%s", err, buf.String())
	}
	if rec["invocation_id"] != "inv-42" {
		t.Errorf("invocation_id mismatch: %v", rec["invocation_id"])
	}
	if rec["function_name"] != "Hello" {
		t.Errorf("function_name mismatch: %v", rec["function_name"])
	}
	if rec["trigger_type"] != "httpTrigger" {
		t.Errorf("trigger_type mismatch: %v", rec["trigger_type"])
	}
	if rec["msg"] != "ping" {
		t.Errorf("msg mismatch: %v", rec["msg"])
	}
}

func TestNewLogHandler_NoInvocationContext(t *testing.T) {
	// Records emitted outside an invocation must not get phantom
	// invocation attributes — just behave like the base handler.
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, nil)
	logger := slog.New(NewLogHandler(base))

	logger.Info("standalone")

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("log output is not valid JSON: %v", err)
	}
	if _, present := rec["invocation_id"]; present {
		t.Errorf("invocation_id should not be present when no IC on context: %v", rec)
	}
}

func TestNewLogHandler_OmitsEmptyFields(t *testing.T) {
	// Half-populated InvocationContext: only invocation_id is set.
	// Other fields must be omitted from the log record entirely (not
	// serialized as empty strings) to keep log volumes tidy.
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, nil)
	logger := slog.New(NewLogHandler(base))

	ctx := NewContext(context.Background(), &InvocationContext{InvocationID: "only-id"})
	logger.InfoContext(ctx, "msg")

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("log output is not valid JSON: %v", err)
	}
	if rec["invocation_id"] != "only-id" {
		t.Errorf("invocation_id mismatch: %v", rec["invocation_id"])
	}
	for _, key := range []string{"function_name", "trigger_type"} {
		if _, present := rec[key]; present {
			t.Errorf("expected %q to be omitted; got %v", key, rec[key])
		}
	}
}

func TestNewLogHandler_PreservesUserAttrs(t *testing.T) {
	// User-supplied attrs (via With) and per-call attrs must coexist with
	// the injected invocation attributes.
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, nil)
	logger := slog.New(NewLogHandler(base)).With("service", "demo")

	ctx := NewContext(context.Background(), &InvocationContext{InvocationID: "inv-77"})
	logger.InfoContext(ctx, "event", "key", "value")

	out := buf.String()
	for _, want := range []string{`"service":"demo"`, `"key":"value"`, `"invocation_id":"inv-77"`} {
		if !strings.Contains(out, want) {
			t.Errorf("log output missing %q\n%s", want, out)
		}
	}
}

func TestNewLogHandler_NilBaseDefaultsToStderr(t *testing.T) {
	// Smoke test: passing nil base must not panic.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("NewLogHandler(nil) panicked: %v", r)
		}
	}()
	if h := NewLogHandler(nil); h == nil {
		t.Fatal("expected non-nil handler from NewLogHandler(nil)")
	}
}
