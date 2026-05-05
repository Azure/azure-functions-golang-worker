package worker

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// loadFunc is a small helper for these tests: registers fn on app and pre-
// populates disp.LoadedFunctions so handleInvocationRequest can look it up.
//
// We register via app.Timer (the simplest non-HTTP trigger) because it has a
// single typed signature func(context.Context, bindings.TimerInfo) error and
// requires no HTTP plumbing in the dispatcher.
func loadFunc(t *testing.T, disp *Dispatcher, name string, fn sdk.TimerHandler) *sdk.RegisteredFunction {
	t.Helper()
	rf := disp.App.Timer(name, fn)

	ft := reflect.TypeOf(fn)
	fields := map[string]*funcField{}
	// Map the trigger input ("timer") to argument position 1 (position 0 is ctx).
	fields["timer"] = &funcField{
		Name:       "timer",
		Type:       ft.In(1),
		Position:   1,
		Direction:  "in",
		IsArgument: true,
	}
	disp.LoadedFunctions.Store(rf.FuncId, &LoadedFunction{
		Function: *rf,
		Fields:   fields,
	})
	return rf
}

// invokeRequest builds a minimal InvocationRequest for the given function ID.
func invokeRequest(funcID, invID string) *pb.InvocationRequest {
	return &pb.InvocationRequest{
		InvocationId: invID,
		FunctionId:   funcID,
		TraceContext: &pb.RpcTraceContext{
			TraceParent: "00-trace-id-span-id-01",
			TraceState:  "vendor=value",
		},
		RetryContext: &pb.RetryContext{
			RetryCount:    1,
			MaxRetryCount: 3,
		},
	}
}

func TestHandleInvocationRequest_PopulatesInvocationContext(t *testing.T) {
	disp := newTestDispatcher("req-1")

	var seen *sdk.InvocationContext
	rf := loadFunc(t, disp, "PopulateIC", func(ctx context.Context, _ bindings.TimerInfo) error {
		ic, _ := sdk.FromContext(ctx)
		seen = ic
		return nil
	})

	resp, err := handleInvocationRequest(invokeRequest(rf.FuncId, "inv-1"), disp, "req-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetInvocationResponse().Result.Status != pb.StatusResult_Success {
		t.Errorf("expected Success, got %v", resp.GetInvocationResponse().Result)
	}
	if seen == nil {
		t.Fatal("user handler did not observe an InvocationContext")
	}
	if seen.InvocationID != "inv-1" {
		t.Errorf("InvocationID = %q, want inv-1", seen.InvocationID)
	}
	if seen.FunctionID != rf.FuncId {
		t.Errorf("FunctionID = %q, want %q", seen.FunctionID, rf.FuncId)
	}
	if seen.FunctionName != "PopulateIC" {
		t.Errorf("FunctionName = %q, want PopulateIC", seen.FunctionName)
	}
	if seen.TriggerType != "timerTrigger" {
		t.Errorf("TriggerType = %q, want timerTrigger", seen.TriggerType)
	}
	if seen.TraceContext.TraceParent != "00-trace-id-span-id-01" {
		t.Errorf("TraceParent mismatch: %q", seen.TraceContext.TraceParent)
	}
	if seen.TraceContext.TraceState != "vendor=value" {
		t.Errorf("TraceState mismatch: %q", seen.TraceContext.TraceState)
	}
	if seen.RetryContext.RetryCount != 1 || seen.RetryContext.MaxRetryCount != 3 {
		t.Errorf("RetryContext mismatch: %+v", seen.RetryContext)
	}
}

func TestHandleInvocationRequest_RunsMiddlewareInOrder(t *testing.T) {
	disp := newTestDispatcher("req-mw")

	var trace []string
	var traceMu sync.Mutex
	add := func(step string) {
		traceMu.Lock()
		trace = append(trace, step)
		traceMu.Unlock()
	}

	disp.App.Use(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, ic *sdk.InvocationContext) error {
			add("A:before")
			err := next(ctx, ic)
			add("A:after")
			return err
		}
	})
	disp.App.Use(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, ic *sdk.InvocationContext) error {
			add("B:before")
			err := next(ctx, ic)
			add("B:after")
			return err
		}
	})

	rf := loadFunc(t, disp, "Ordered", func(ctx context.Context, _ bindings.TimerInfo) error {
		add("user")
		return nil
	})

	if _, err := handleInvocationRequest(invokeRequest(rf.FuncId, "inv-mw"), disp, "req-mw"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"A:before", "B:before", "user", "B:after", "A:after"}
	if !reflect.DeepEqual(trace, want) {
		t.Errorf("ordering mismatch:\n got %v\nwant %v", trace, want)
	}
}

func TestHandleInvocationRequest_MiddlewareCanShortCircuit(t *testing.T) {
	disp := newTestDispatcher("req-sc")

	gateErr := errors.New("not authorized")
	disp.App.Use(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, ic *sdk.InvocationContext) error {
			return gateErr
		}
	})

	var userCalls atomic.Int32
	rf := loadFunc(t, disp, "ShortCircuit", func(ctx context.Context, _ bindings.TimerInfo) error {
		userCalls.Add(1)
		return nil
	})

	resp, err := handleInvocationRequest(invokeRequest(rf.FuncId, "inv-sc"), disp, "req-sc")
	if err != nil {
		t.Fatalf("unexpected outer error: %v", err)
	}
	if userCalls.Load() != 0 {
		t.Errorf("user handler must not be called when middleware short-circuits; called %d times", userCalls.Load())
	}
	status := resp.GetInvocationResponse().Result
	if status.Status != pb.StatusResult_Failure {
		t.Errorf("expected Failure status from short-circuit; got %v", status.Status)
	}
	if status.Exception == nil || status.Exception.Message != gateErr.Error() {
		t.Errorf("expected exception message %q on InvocationResponse; got %+v", gateErr.Error(), status.Exception)
	}
}

func TestHandleInvocationRequest_MiddlewareCanEnrichContext(t *testing.T) {
	disp := newTestDispatcher("req-enrich")

	type ctxKey struct{}
	disp.App.Use(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, ic *sdk.InvocationContext) error {
			ctx = context.WithValue(ctx, ctxKey{}, "from-middleware")
			return next(ctx, ic)
		}
	})

	var observed string
	rf := loadFunc(t, disp, "Enriched", func(ctx context.Context, _ bindings.TimerInfo) error {
		if v, ok := ctx.Value(ctxKey{}).(string); ok {
			observed = v
		}
		return nil
	})

	if _, err := handleInvocationRequest(invokeRequest(rf.FuncId, "inv-enrich"), disp, "req-enrich"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if observed != "from-middleware" {
		t.Errorf("user handler did not observe middleware-enriched ctx; got %q", observed)
	}
}

func TestHandleInvocationRequest_UserErrorBecomesFailureStatus(t *testing.T) {
	disp := newTestDispatcher("req-err")

	wantErr := errors.New("user blew up")
	rf := loadFunc(t, disp, "FailingFn", func(ctx context.Context, _ bindings.TimerInfo) error {
		return wantErr
	})

	resp, err := handleInvocationRequest(invokeRequest(rf.FuncId, "inv-err"), disp, "req-err")
	if err != nil {
		t.Fatalf("unexpected outer error: %v", err)
	}
	status := resp.GetInvocationResponse().Result
	if status.Status != pb.StatusResult_Failure {
		t.Errorf("expected Failure; got %v", status.Status)
	}
	if status.Exception == nil || status.Exception.Message != wantErr.Error() {
		t.Errorf("expected exception with user error message; got %+v", status.Exception)
	}
}

func TestHandleInvocationRequest_NoMiddleware_StillRuns(t *testing.T) {
	// Smoke test: with no middleware registered the user function is still
	// called exactly once and Success is reported.
	disp := newTestDispatcher("req-none")

	var calls atomic.Int32
	rf := loadFunc(t, disp, "Plain", func(ctx context.Context, _ bindings.TimerInfo) error {
		calls.Add(1)
		return nil
	})

	resp, err := handleInvocationRequest(invokeRequest(rf.FuncId, "inv-none"), disp, "req-none")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls.Load() != 1 {
		t.Errorf("user handler called %d times; want 1", calls.Load())
	}
	if resp.GetInvocationResponse().Result.Status != pb.StatusResult_Success {
		t.Errorf("expected Success; got %v", resp.GetInvocationResponse().Result.Status)
	}
}
