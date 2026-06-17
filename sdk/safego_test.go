package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"
)

// waitTimeout blocks until done is closed or the deadline elapses, failing the
// test on timeout so a broken helper can never hang the suite.
func waitTimeout(t *testing.T, done <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", msg)
	}
}

func TestRecover_NoPanicIsNoop(t *testing.T) {
	// Deferring Recover when no panic is in flight must do nothing.
	// If this returns without hanging or panicking, it passed.
	func() {
		defer Recover(context.Background())
		// no panic
	}()
}

func TestRecover_CatchesPanicInGoroutine(t *testing.T) {
	// A goroutine that defers Recover must not crash the process on panic.
	done := make(chan struct{})
	go func() {
		defer Recover(context.Background())
		defer func() { close(done) }()
		panic("boom")
	}()
	waitTimeout(t, done, "goroutine with Recover to complete")
}

func TestRecover_LogsViaSlog(t *testing.T) {
	// Recover must log the panic at error level through slog so the SDK log
	// handler can attach invocation metadata. Verify by capturing output.
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})
	prevDefault := slog.Default()
	slog.SetDefault(slog.New(NewLogHandler(base)))
	t.Cleanup(func() { slog.SetDefault(prevDefault) })

	ctx := NewContext(context.Background(), &InvocationContext{
		InvocationID: "inv-99",
		FunctionName: "EventHubHandler",
		TriggerType:  "eventHubTrigger",
	})

	// Run Recover via a deferred call that actually panics.
	func() {
		defer Recover(ctx)
		panic("nil pointer-ish")
	}()

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("log output is not valid JSON: %v\n%s", err, buf.String())
	}
	if rec["level"] != "ERROR" {
		t.Errorf("expected ERROR level, got %v", rec["level"])
	}
	if rec["invocation_id"] != "inv-99" {
		t.Errorf("expected invocation metadata on the panic log, got %v", rec["invocation_id"])
	}
	if rec["panic"] != "nil pointer-ish" {
		t.Errorf("expected the recovered value in the log, got %v", rec["panic"])
	}
	if s, ok := rec["stack"].(string); !ok || s == "" {
		t.Errorf("expected a non-empty stack attribute, got %v", rec["stack"])
	}
}

func TestRecoverTo_SetsErrorOnPanic(t *testing.T) {
	// RecoverTo must capture the panic as an error via the provided pointer.
	fn := func() (err error) {
		defer RecoverTo(context.Background(), &err)
		panic("kaboom")
	}
	err := fn()
	if err == nil {
		t.Fatal("expected non-nil error after panic")
	}
	if got := err.Error(); got != "recovered panic: kaboom" {
		t.Errorf("unexpected error message: %s", got)
	}
}

func TestRecoverTo_PreservesExistingError(t *testing.T) {
	// If *errp is already non-nil when the panic fires, RecoverTo must not
	// overwrite it — the original error is the more meaningful signal.
	original := fmt.Errorf("original error")
	fn := func() (err error) {
		defer RecoverTo(context.Background(), &err)
		err = original
		panic("secondary boom")
	}
	err := fn()
	if err != original {
		t.Errorf("expected original error to be preserved, got: %v", err)
	}
}

func TestRecoverTo_NoPanicIsNoop(t *testing.T) {
	// When no panic occurs, RecoverTo must leave *errp untouched.
	fn := func() (err error) {
		defer RecoverTo(context.Background(), &err)
		return nil
	}
	if err := fn(); err != nil {
		t.Errorf("expected nil error when no panic, got: %v", err)
	}
}

func TestRecoverTo_NoPanicPreservesReturnedError(t *testing.T) {
	// When the function returns a real error and no panic happens,
	// RecoverTo must not interfere.
	want := fmt.Errorf("real error")
	fn := func() (err error) {
		defer RecoverTo(context.Background(), &err)
		return want
	}
	if got := fn(); got != want {
		t.Errorf("expected %v, got %v", want, got)
	}
}
