package worker

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// TestRunUserInvocation_Success exercises the happy path: no error, no
// panic. All three return values must be zero so the caller can compose
// Success without extra branches.
func TestRunUserInvocation_Success(t *testing.T) {
	inner := func(_ context.Context, _ *sdk.MiddlewareContext) error { return nil }
	rec, stack, err := runUserInvocation(context.Background(), &sdk.MiddlewareContext{}, nil, inner)
	if err != nil || rec != nil || stack != "" {
		t.Errorf("expected (nil, \"\", nil); got (%v, %q, %v)", rec, stack, err)
	}
}

// TestRunUserInvocation_Error verifies returned errors flow through to the
// caller untouched so statusFromInvocation can attribute them.
func TestRunUserInvocation_Error(t *testing.T) {
	wantErr := errors.New("boom")
	inner := func(_ context.Context, _ *sdk.MiddlewareContext) error { return wantErr }
	rec, stack, err := runUserInvocation(context.Background(), &sdk.MiddlewareContext{}, nil, inner)
	if !errors.Is(err, wantErr) {
		t.Errorf("err = %v; want %v", err, wantErr)
	}
	if rec != nil || stack != "" {
		t.Errorf("expected recovered/stack to be zero on plain error; got rec=%v stack=%q", rec, stack)
	}
}

// TestRunUserInvocation_Panic verifies panics are converted into recovered
// values plus a non-empty stack trace, with err nil. Critical for issue #8:
// without this, the gRPC-body path would never produce an InvocationResponse
// when the user function panics.
func TestRunUserInvocation_Panic(t *testing.T) {
	inner := func(_ context.Context, _ *sdk.MiddlewareContext) error {
		panic("nope")
	}
	rec, stack, err := runUserInvocation(context.Background(), &sdk.MiddlewareContext{}, nil, inner)
	if err != nil {
		t.Errorf("err = %v; want nil on panic", err)
	}
	if rec != "nope" {
		t.Errorf("recovered = %v; want %q", rec, "nope")
	}
	if !strings.Contains(stack, "runUserInvocation") {
		t.Errorf("expected stack to include runUserInvocation frame; got %q", stack)
	}
}

// TestStatusFromInvocation covers the three branches statusFromInvocation
// dispatches on: panic (highest priority), error, and success. The
// resulting StatusResult goes straight onto InvocationResponse, so its
// shape is the host-visible contract.
func TestStatusFromInvocation(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		s := statusFromInvocation(nil, "", nil)
		if s.Status != pb.StatusResult_Success {
			t.Errorf("status = %v; want Success", s.Status)
		}
		if s.Exception != nil {
			t.Errorf("expected nil Exception on success; got %+v", s.Exception)
		}
	})

	t.Run("error", func(t *testing.T) {
		s := statusFromInvocation(nil, "", errors.New("oops"))
		if s.Status != pb.StatusResult_Failure {
			t.Errorf("status = %v; want Failure", s.Status)
		}
		if s.Exception == nil || s.Exception.Message != "oops" {
			t.Errorf("Exception.Message = %+v; want oops", s.Exception)
		}
		if !s.Exception.IsUserException {
			t.Errorf("IsUserException = false; want true for user error")
		}
		if s.Exception.StackTrace != "" {
			t.Errorf("StackTrace = %q; want empty for non-panic error", s.Exception.StackTrace)
		}
	})

	t.Run("panic_takes_precedence_over_error", func(t *testing.T) {
		// Defense: if both surface (it shouldn't), the panic shape wins
		// because that indicates the user code never returned normally.
		s := statusFromInvocation("real_cause", "stack-frames-here", errors.New("shadowed"))
		if s.Status != pb.StatusResult_Failure {
			t.Errorf("status = %v; want Failure", s.Status)
		}
		if s.Exception.Message != "real_cause" {
			t.Errorf("Exception.Message = %q; want real_cause", s.Exception.Message)
		}
		if s.Exception.StackTrace != "stack-frames-here" {
			t.Errorf("StackTrace = %q; want stack-frames-here", s.Exception.StackTrace)
		}
	})
}
