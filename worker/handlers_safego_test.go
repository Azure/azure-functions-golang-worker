package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// waitCh is a test safety net — it blocks until a channel is closed
// (signaling a goroutine did its work) or fails the test after 2 seconds
// so a broken goroutine can't hang the suite forever.
func waitCh(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch: // goroutine signaled success
	case <-time.After(2 * time.Second): // goroutine never finished — fail loud
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestHandleInvocation_SdkGo_HappyPath verifies that a handler spawning
// background work via sdk.Go completes successfully and the goroutine
// runs to completion without crashing the worker.
func TestHandleInvocation_SdkGo_HappyPath(t *testing.T) {
	disp := newTestDispatcher("req-safego-ok")

	// The handler fires a background goroutine via sdk.Go that signals done.
	done := make(chan struct{})
	rf := loadFunc(t, disp, "SafeGoOK", func(ctx context.Context, _ bindings.TimerInfo) error {
		sdk.Go(ctx, func() {
			close(done) // signals the goroutine ran
		})
		return nil
	})

	// Drive the full handleInvocationRequest path — same as the real dispatcher.
	resp, err := handleInvocationRequest(invokeRequest(rf.FuncId, "inv-safego-ok"), disp, "req-safego-ok")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Handler returned nil, so the invocation must report Success.
	status := resp.GetInvocationResponse().Result
	if status.Status != pb.StatusResult_Success {
		t.Errorf("expected Success, got %v: %v", status.Status, status.Exception)
	}

	// Confirm the background goroutine actually executed.
	waitCh(t, done, "sdk.Go goroutine to complete")
}

// TestHandleInvocation_SdkGo_PanicDoesNotCrashWorker verifies the core
// guarantee: a panic inside a sdk.Go goroutine is recovered, the
// invocation itself succeeds (the handler returned nil before the
// goroutine panicked), and the worker process stays alive.
func TestHandleInvocation_SdkGo_PanicDoesNotCrashWorker(t *testing.T) {
	// Swap in a custom panic handler so we can capture the recovered value
	// without relying on slog output. Restore the default on cleanup.
	t.Cleanup(func() { sdk.SetGoroutinePanicHandler(nil) })

	var (
		mu       sync.Mutex
		gotPanic any
		handled  = make(chan struct{})
	)
	sdk.SetGoroutinePanicHandler(func(_ context.Context, recovered any, _ []byte) {
		mu.Lock()
		gotPanic = recovered
		mu.Unlock()
		close(handled) // unblock the test once the panic is observed
	})

	disp := newTestDispatcher("req-safego-panic")

	// The handler spawns a goroutine that panics, but returns nil itself.
	// Without sdk.Go the panic would tear down the process.
	rf := loadFunc(t, disp, "SafeGoPanic", func(ctx context.Context, _ bindings.TimerInfo) error {
		sdk.Go(ctx, func() {
			panic("background boom")
		})
		return nil // handler succeeds; panic is isolated to the goroutine
	})

	// Run through the real dispatch path.
	resp, err := handleInvocationRequest(invokeRequest(rf.FuncId, "inv-safego-panic"), disp, "req-safego-panic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Invocation must succeed — the handler returned nil before the goroutine panicked.
	status := resp.GetInvocationResponse().Result
	if status.Status != pb.StatusResult_Success {
		t.Errorf("expected Success (handler returned nil), got %v: %v", status.Status, status.Exception)
	}

	// Wait for sdk.Go to recover the panic and route it to our handler.
	waitCh(t, handled, "panic handler to fire")

	// Verify the correct panic value was captured.
	mu.Lock()
	defer mu.Unlock()
	if gotPanic != "background boom" {
		t.Errorf("recovered value = %v; want %q", gotPanic, "background boom")
	}
}

// TestHandleInvocation_SdkRecover_PanicDoesNotCrashWorker exercises the
// sdk.Recover path: a handler launches a manual goroutine with
// defer sdk.Recover(ctx), that goroutine panics, and the worker stays up.
func TestHandleInvocation_SdkRecover_PanicDoesNotCrashWorker(t *testing.T) {
	// Same custom-handler pattern as the sdk.Go test above.
	t.Cleanup(func() { sdk.SetGoroutinePanicHandler(nil) })

	var (
		mu       sync.Mutex
		gotPanic any
		handled  = make(chan struct{})
	)
	sdk.SetGoroutinePanicHandler(func(_ context.Context, recovered any, _ []byte) {
		mu.Lock()
		gotPanic = recovered
		mu.Unlock()
		close(handled)
	})

	disp := newTestDispatcher("req-recover-panic")

	// This handler uses the WaitGroup + defer sdk.Recover pattern from
	// the README — a manual goroutine rather than sdk.Go.
	rf := loadFunc(t, disp, "RecoverPanic", func(ctx context.Context, _ bindings.TimerInfo) error {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer sdk.Recover(ctx) // catches the panic, keeps the worker alive
			defer wg.Done()        // unblocks wg.Wait even after a panic
			panic("manual goroutine boom")
		}()
		wg.Wait() // returns once the goroutine exits (panic recovered)
		return nil
	})

	// Drive the real dispatch path.
	resp, err := handleInvocationRequest(invokeRequest(rf.FuncId, "inv-recover-panic"), disp, "req-recover-panic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Handler returned nil after wg.Wait, so invocation must be Success.
	status := resp.GetInvocationResponse().Result
	if status.Status != pb.StatusResult_Success {
		t.Errorf("expected Success (handler returned nil after wg.Wait), got %v: %v", status.Status, status.Exception)
	}

	// Confirm the panic was routed to our handler, not swallowed silently.
	waitCh(t, handled, "panic handler to fire from manual goroutine")

	mu.Lock()
	defer mu.Unlock()
	if gotPanic != "manual goroutine boom" {
		t.Errorf("recovered value = %v; want %q", gotPanic, "manual goroutine boom")
	}
}
