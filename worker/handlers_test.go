package worker

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

func newTestApp() *sdk.App {
	return sdk.FunctionApp()
}

// --- FunctionsMetadataRequest tests ---

func TestHandleFunctionsMetadataRequest_Empty(t *testing.T) {
	app := newTestApp()
	resp := handleFunctionsMetadataRequest(&pb.FunctionsMetadataRequest{}, app, "req-1")

	metaResp := resp.GetContent().(*pb.StreamingMessage_FunctionMetadataResponse).FunctionMetadataResponse
	if len(metaResp.FunctionMetadataResults) != 0 {
		t.Errorf("expected 0 functions, got %d", len(metaResp.FunctionMetadataResults))
	}
}

func TestHandleFunctionsMetadataRequest_WithFunction(t *testing.T) {
	app := newTestApp()
	app.HTTP("hello", sdk.HTTPHandler(func(w http.ResponseWriter, r *http.Request) {}),
		sdk.WithMethods("GET"), sdk.WithAuth("anonymous"))

	resp := handleFunctionsMetadataRequest(&pb.FunctionsMetadataRequest{}, app, "req-1")
	metaResp := resp.GetContent().(*pb.StreamingMessage_FunctionMetadataResponse).FunctionMetadataResponse

	if len(metaResp.FunctionMetadataResults) != 1 {
		t.Fatalf("expected 1 function, got %d", len(metaResp.FunctionMetadataResults))
	}

	meta := metaResp.FunctionMetadataResults[0]
	if meta.Language != "go" {
		t.Errorf("expected language %q, got %q", "go", meta.Language)
	}
}

// --- InvocationRequest tests ---

func TestHandleInvocationRequest_FunctionNotLoaded(t *testing.T) {
	app := newTestApp()
	disp := newTestDispatcher("req-1")
	disp.App = app

	_, err := handleInvocationRequest(&pb.InvocationRequest{
		FunctionId:   "nonexistent",
		InvocationId: "inv-1",
	}, disp, "req-1")

	if err == nil {
		t.Fatal("expected error for unloaded function")
	}
}

func TestHandleInvocationRequest_SimpleHTTP(t *testing.T) {
	app := newTestApp()
	disp := newTestDispatcher("req-1")
	disp.App = app

	handler := sdk.HTTPHandler(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello"))
	})
	app.HTTP("test", handler, sdk.WithMethods("GET"))

	// Get function ID
	var funcID string
	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		funcID = key.(string)
		return false
	})

	// Load the function
	loadResp := handleFunctionLoadRequest(&pb.FunctionLoadRequest{
		FunctionId: funcID,
	}, disp, "req-1")
	loadResult := loadResp.GetContent().(*pb.StreamingMessage_FunctionLoadResponse).FunctionLoadResponse
	if loadResult.Result.Status != pb.StatusResult_Success {
		t.Fatalf("expected load success, got %v", loadResult.Result.Status)
	}

	// Invoke
	resp, err := handleInvocationRequest(&pb.InvocationRequest{
		FunctionId:   funcID,
		InvocationId: "inv-1",
		InputData: []*pb.ParameterBinding{
			{
				Name: "req",
				RpcData: &pb.ParameterBinding_Data{
					Data: &pb.TypedData{
						Data: &pb.TypedData_Http{
							Http: &pb.RpcHttp{
								Method: "GET",
								Url:    "http://localhost/api/test",
							},
						},
					},
				},
			},
		},
	}, disp, "req-1")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	invResp := resp.GetContent().(*pb.StreamingMessage_InvocationResponse).InvocationResponse
	if invResp.Result.Status != pb.StatusResult_Success {
		t.Errorf("expected Success, got %v: %v", invResp.Result.Status, invResp.Result.Exception)
	}
}

func TestHandleInvocationRequest_TimerWithError(t *testing.T) {
	app := newTestApp()
	disp := newTestDispatcher("req-1")
	disp.App = app

	handler := sdk.TimerHandler(func(ctx context.Context, timer bindings.TimerInfo) error {
		return context.DeadlineExceeded
	})
	app.Timer("tick", handler, sdk.WithSchedule("0 * * * * *"))

	var funcID string
	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		funcID = key.(string)
		return false
	})

	handleFunctionLoadRequest(&pb.FunctionLoadRequest{FunctionId: funcID}, disp, "req-1")

	resp, err := handleInvocationRequest(&pb.InvocationRequest{
		FunctionId:   funcID,
		InvocationId: "inv-1",
		InputData: []*pb.ParameterBinding{
			{
				Name: "timer",
				RpcData: &pb.ParameterBinding_Data{
					Data: &pb.TypedData{
						Data: &pb.TypedData_Json{Json: `{"schedule":{},"scheduleStatus":{"last":"","next":""},"isPastDue":false}`},
					},
				},
			},
		},
	}, disp, "req-1")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	invResp := resp.GetContent().(*pb.StreamingMessage_InvocationResponse).InvocationResponse
	if invResp.Result.Status != pb.StatusResult_Failure {
		t.Errorf("expected Failure, got %v", invResp.Result.Status)
	}
}

// --- ServiceBus trigger metadata test ---

func TestHandleFunctionsMetadataRequest_ServiceBus(t *testing.T) {
	app := newTestApp()
	app.ServiceBusQueue("sbFunc", sdk.ServiceBusHandler(func(ctx context.Context, msg bindings.ServiceBusMessage) error {
		return nil
	}), sdk.WithQueueName("myqueue"), sdk.WithConnection("SBConn"))

	resp := handleFunctionsMetadataRequest(&pb.FunctionsMetadataRequest{}, app, "req-1")
	metaResp := resp.GetContent().(*pb.StreamingMessage_FunctionMetadataResponse).FunctionMetadataResponse

	if len(metaResp.FunctionMetadataResults) != 1 {
		t.Fatalf("expected 1 function, got %d", len(metaResp.FunctionMetadataResults))
	}

	meta := metaResp.FunctionMetadataResults[0]
	triggerBinding, ok := meta.Bindings["message"]
	if !ok {
		t.Fatal("expected 'message' binding")
	}
	if triggerBinding.Type != "serviceBusTrigger" {
		t.Errorf("expected type %q, got %q", "serviceBusTrigger", triggerBinding.Type)
	}
	if triggerBinding.Direction != pb.BindingInfo_in {
		t.Errorf("expected direction 'in', got %v", triggerBinding.Direction)
	}
}

// --- Middleware-aware InvocationRequest tests ---

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

	disp.App.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
			add("A:before")
			err := next(ctx, mc)
			add("A:after")
			return err
		}
	}))
	disp.App.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
			add("B:before")
			err := next(ctx, mc)
			add("B:after")
			return err
		}
	}))

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
	disp.App.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
			return gateErr
		}
	}))

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
	disp.App.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
			ctx = context.WithValue(ctx, ctxKey{}, "from-middleware")
			return next(ctx, mc)
		}
	}))

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

// TestHandleInvocationRequest_UserPanicBecomesFailureStatus is the
// regression test for the gRPC-body panic-recovery path. Before issue #8
// was fixed, a panicking user function would unwind into the dispatcher
// goroutine, which only logged the panic — the host then timed out the
// invocation because no InvocationResponse ever shipped. The shared
// runUserInvocation helper now turns panics into Failure responses.
func TestHandleInvocationRequest_UserPanicBecomesFailureStatus(t *testing.T) {
	disp := newTestDispatcher("req-panic")

	rf := loadFunc(t, disp, "PanickyFn", func(ctx context.Context, _ bindings.TimerInfo) error {
		panic("boom: user code blew up")
	})

	resp, err := handleInvocationRequest(invokeRequest(rf.FuncId, "inv-panic"), disp, "req-panic")
	if err != nil {
		t.Fatalf("expected nil outer error so the host receives a response; got %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil InvocationResponse so the host does not time out")
	}
	status := resp.GetInvocationResponse().Result
	if status.Status != pb.StatusResult_Failure {
		t.Fatalf("expected Failure on panic; got %v", status.Status)
	}
	if status.Exception == nil {
		t.Fatal("expected RpcException populated from recovered panic; got nil")
	}
	if status.Exception.Message != "boom: user code blew up" {
		t.Errorf("expected panic message in exception; got %q", status.Exception.Message)
	}
	if status.Exception.Source != "User function" {
		t.Errorf("expected Source=User function; got %q", status.Exception.Source)
	}
	if !status.Exception.IsUserException {
		t.Errorf("expected IsUserException=true for user-code panic")
	}
	if status.Exception.StackTrace == "" {
		t.Errorf("expected non-empty stack trace on recovered panic")
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

func TestHandleInvocationRequest_OutboundTraceAttributes_ForwardedToResponse(t *testing.T) {
	// Verifies the dispatcher copies the MiddlewareContext's outbound
	// trace attributes onto InvocationResponse.TraceContextAttributes
	// verbatim (no auto-population or filtering at the dispatcher layer)
	// so the host can apply them as tags on its parent span. The handler
	// simulates a middleware writer via
	// [sdk.MiddlewareContext.SetOutboundTraceAttribute]; the dispatcher
	// itself doesn't care who wrote the entries.
	disp := newTestDispatcher("req-tags")

	rf := loadFunc(t, disp, "TagSetter", func(ctx context.Context, _ bindings.TimerInfo) error {
		mc, ok := sdk.MiddlewareContextFrom(ctx)
		if !ok {
			t.Errorf("expected MiddlewareContext on ctx")
			return nil
		}
		mc.SetOutboundTraceAttribute("tenant", "contoso")
		mc.SetOutboundTraceAttribute("result.kind", "ok")
		return nil
	})

	resp, err := handleInvocationRequest(invokeRequest(rf.FuncId, "inv-tags"), disp, "req-tags")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := resp.GetInvocationResponse().GetTraceContextAttributes()
	if got["tenant"] != "contoso" {
		t.Errorf("tenant: got %q want %q", got["tenant"], "contoso")
	}
	if got["result.kind"] != "ok" {
		t.Errorf("result.kind: got %q want %q", got["result.kind"], "ok")
	}
	if len(got) != 2 {
		t.Errorf("expected exactly 2 entries forwarded, got %d (%v)", len(got), got)
	}
}
