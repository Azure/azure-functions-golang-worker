package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
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

func TestGo_RunsFn(t *testing.T) {
	done := make(chan struct{})
	Go(context.Background(), func() { close(done) })
	waitTimeout(t, done, "fn to run")
}

func TestGo_RecoversPanic(t *testing.T) {
	// A panic inside the goroutine must be recovered (not crash the test
	// process) and routed to the panic handler.
	prev := panicHandlerHolder.Load()
	t.Cleanup(func() { panicHandlerHolder.Store(prev) })

	var (
		mu       sync.Mutex
		gotPanic any
		gotStack []byte
		handled  = make(chan struct{})
	)
	SetGoroutinePanicHandler(func(_ context.Context, recovered any, stack []byte) {
		mu.Lock()
		gotPanic = recovered
		gotStack = stack
		mu.Unlock()
		close(handled)
	})

	Go(context.Background(), func() { panic("boom") })

	waitTimeout(t, handled, "panic handler to be invoked")

	mu.Lock()
	defer mu.Unlock()
	if gotPanic != "boom" {
		t.Errorf("recovered value mismatch: got %v, want \"boom\"", gotPanic)
	}
	if len(gotStack) == 0 {
		t.Error("expected a non-empty stack trace")
	}
}

func TestGo_NilFnIsNoop(t *testing.T) {
	// Must not panic or spawn anything. If this returns, it passed.
	Go(context.Background(), nil)
}

func TestRecover_NoPanicIsNoop(t *testing.T) {
	// Deferring Recover when no panic is in flight must do nothing and must
	// not invoke the panic handler.
	prev := panicHandlerHolder.Load()
	t.Cleanup(func() { panicHandlerHolder.Store(prev) })

	called := false
	SetGoroutinePanicHandler(func(context.Context, any, []byte) { called = true })

	func() {
		defer Recover(context.Background())
		// no panic
	}()

	if called {
		t.Error("panic handler should not be called when there is no panic")
	}
}

func TestRecover_GuardsManualGoroutine(t *testing.T) {
	prev := panicHandlerHolder.Load()
	t.Cleanup(func() { panicHandlerHolder.Store(prev) })

	handled := make(chan struct{})
	SetGoroutinePanicHandler(func(context.Context, any, []byte) { close(handled) })

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer Recover(context.Background())
		defer wg.Done()
		panic("manual goroutine boom")
	}()
	wg.Wait()

	waitTimeout(t, handled, "panic handler from manual goroutine")
}

func TestSetGoroutinePanicHandler_NilRestoresDefault(t *testing.T) {
	prev := panicHandlerHolder.Load()
	t.Cleanup(func() { panicHandlerHolder.Store(prev) })

	SetGoroutinePanicHandler(func(context.Context, any, []byte) {})
	if panicHandlerHolder.Load() == nil {
		t.Fatal("expected a custom handler to be installed")
	}

	SetGoroutinePanicHandler(nil)
	if panicHandlerHolder.Load() != nil {
		t.Error("expected nil to clear the custom handler (fall back to default)")
	}
}

func TestDefaultGoroutinePanicHandler_LogsViaSlog(t *testing.T) {
	// The default handler logs the panic at error level through slog so the
	// sdk log handler can attach invocation metadata. Capture it via a buffer
	// base handler wrapped by NewLogHandler, with an InvocationContext on ctx.
	// Invoke the default handler directly (rather than through Go) so the
	// assertion is deterministic and free of goroutine timing.
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

	defaultGoroutinePanicHandler(ctx, "nil pointer-ish", []byte("goroutine 1 [running]:\n..."))

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
