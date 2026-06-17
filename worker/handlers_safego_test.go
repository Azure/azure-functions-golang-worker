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
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestHandleInvocation_SdkRecover_PanicDoesNotCrashWorker verifies the core
// guarantee: a handler launches a manual goroutine with defer sdk.Recover(ctx),
// that goroutine panics, and the worker process stays alive.
func TestHandleInvocation_SdkRecover_PanicDoesNotCrashWorker(t *testing.T) {
	disp := newTestDispatcher("req-recover-panic")

	// The handler uses the WaitGroup + defer sdk.Recover pattern.
	// The goroutine panics, Recover catches it, the handler returns nil.
	handled := make(chan struct{})
	rf := loadFunc(t, disp, "RecoverPanic", func(ctx context.Context, _ bindings.TimerInfo) error {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer sdk.Recover(ctx)
			defer wg.Done()
			defer func() { close(handled) }()
			panic("manual goroutine boom")
		}()
		wg.Wait()
		return nil
	})

	resp, err := handleInvocationRequest(invokeRequest(rf.FuncId, "inv-recover-panic"), disp, "req-recover-panic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Handler returned nil after wg.Wait, so invocation must be Success.
	status := resp.GetInvocationResponse().Result
	if status.Status != pb.StatusResult_Success {
		t.Errorf("expected Success (handler returned nil after wg.Wait), got %v: %v", status.Status, status.Exception)
	}

	// Confirm the goroutine ran (and its panic was recovered, not skipped).
	waitCh(t, handled, "panic handler to fire from manual goroutine")
}

// TestHandleInvocation_SdkRecover_HappyPath verifies that a goroutine
// guarded by sdk.Recover that does NOT panic completes normally.
func TestHandleInvocation_SdkRecover_HappyPath(t *testing.T) {
	disp := newTestDispatcher("req-recover-ok")

	done := make(chan struct{})
	rf := loadFunc(t, disp, "RecoverOK", func(ctx context.Context, _ bindings.TimerInfo) error {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer sdk.Recover(ctx)
			defer wg.Done()
			close(done)
		}()
		wg.Wait()
		return nil
	})

	resp, err := handleInvocationRequest(invokeRequest(rf.FuncId, "inv-recover-ok"), disp, "req-recover-ok")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	status := resp.GetInvocationResponse().Result
	if status.Status != pb.StatusResult_Success {
		t.Errorf("expected Success, got %v: %v", status.Status, status.Exception)
	}

	waitCh(t, done, "guarded goroutine to complete")
}

// TestHandleInvocation_SdkRecoverTo_PanicFailsInvocation verifies that a
// panicking goroutine guarded by sdk.RecoverTo propagates the panic as a
// failed invocation (Failure status), enabling host retries.
func TestHandleInvocation_SdkRecoverTo_PanicFailsInvocation(t *testing.T) {
	disp := newTestDispatcher("req-recoverto-panic")

	handled := make(chan struct{})
	rf := loadFunc(t, disp, "RecoverToPanic", func(ctx context.Context, _ bindings.TimerInfo) error {
		// Simulate the errgroup pattern: goroutine panics, RecoverTo
		// captures it as an error, and the handler returns that error.
		//
		// Defer ordering matters: wg.Done is registered first so it runs
		// LAST (after RecoverTo has set err), ensuring the parent reads
		// the error only after it's been assigned.
		var err error
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer sdk.RecoverTo(ctx, &err)
			defer func() { close(handled) }()
			panic("recoverTo goroutine boom")
		}()
		wg.Wait()
		return err
	})

	resp, respErr := handleInvocationRequest(invokeRequest(rf.FuncId, "inv-recoverto-panic"), disp, "req-recoverto-panic")
	if respErr != nil {
		t.Fatalf("unexpected error: %v", respErr)
	}

	status := resp.GetInvocationResponse().Result
	if status.Status != pb.StatusResult_Failure {
		t.Errorf("expected Failure (panic propagated via RecoverTo), got %v", status.Status)
	}

	waitCh(t, handled, "RecoverTo goroutine to complete")
}
