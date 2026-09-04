package worker

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// TestEncodeReturnValue covers the encoding the dispatcher applies to a
// non-HTTP function's return value (or a value a middleware records via
// mc.SetReturnValue): strings and bytes pass through as the matching
// TypedData kind; everything else is JSON-encoded; nil yields no TypedData.
func TestEncodeReturnValue(t *testing.T) {
	if td := encodeReturnValue(nil); td != nil {
		t.Errorf("nil → %v, want nil", td)
	}
	if td := encodeReturnValue("hi"); td.GetString_() != "hi" {
		t.Errorf("string → %q, want hi", td.GetString_())
	}
	if td := encodeReturnValue([]byte("bytes")); string(td.GetBytes()) != "bytes" {
		t.Errorf("[]byte → %q, want bytes", td.GetBytes())
	}
	type payload struct {
		A int `json:"a"`
	}
	if td := encodeReturnValue(payload{A: 7}); td.GetJson() != `{"a":7}` {
		t.Errorf("struct → %q, want {\"a\":7}", td.GetJson())
	}
}

// TestHandleInvocationRequest_MiddlewareSetsReturnValue verifies the seam
// durable orchestration relies on: a middleware that short-circuits the
// chain and records a return value via mc.SetReturnValue has that value
// encoded into InvocationResponse.ReturnValue, and the user function is
// never invoked.
func TestHandleInvocationRequest_MiddlewareSetsReturnValue(t *testing.T) {
	disp := newTestDispatcher("req-rv")

	const want = "AAECAwQF" // stand-in for a base64 orchestrator response
	disp.App.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
			mc.SetReturnValue(want)
			return nil // short-circuit: do not call next
		}
	}))

	var userCalls atomic.Int32
	rf := loadFunc(t, disp, "RVShortCircuit", func(ctx context.Context, _ bindings.TimerInfo) error {
		userCalls.Add(1)
		return nil
	})

	resp, err := handleInvocationRequest(invokeRequest(rf.FuncId, "inv-rv"), disp, "req-rv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if userCalls.Load() != 0 {
		t.Errorf("user handler must not run when middleware short-circuits; ran %d times", userCalls.Load())
	}
	ir := resp.GetInvocationResponse()
	if ir.Result.Status != pb.StatusResult_Success {
		t.Errorf("status = %v, want Success", ir.Result.Status)
	}
	if ir.ReturnValue.GetString_() != want {
		t.Errorf("ReturnValue = %q, want %q", ir.ReturnValue.GetString_(), want)
	}
}

// TestHandleInvocationRequest_InputBytesAvailableToMiddleware verifies the
// dispatcher surfaces the raw trigger payload on mc.InputBytes so a
// middleware (e.g. durable orchestration replay) can read it directly.
func TestHandleInvocationRequest_InputBytesAvailableToMiddleware(t *testing.T) {
	disp := newTestDispatcher("req-in")

	const payload = "orchestration-history-base64"
	var seen string
	disp.App.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
			seen = string(mc.InputBytes())
			return next(ctx, mc)
		}
	}))

	rf := loadFunc(t, disp, "InputCapture", func(ctx context.Context, _ bindings.TimerInfo) error {
		return nil
	})

	req := invokeRequest(rf.FuncId, "inv-in")
	req.InputData = []*pb.ParameterBinding{{
		Name: "timer", // matches the timer trigger's "in" binding name
		RpcData: &pb.ParameterBinding_Data{
			Data: &pb.TypedData{Data: &pb.TypedData_String_{String_: payload}},
		},
	}}

	if _, err := handleInvocationRequest(req, disp, "req-in"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seen != payload {
		t.Errorf("mc.InputBytes() = %q, want %q", seen, payload)
	}
}

// TestHandleInvocationRequest_BindingInputsAvailableToMiddleware verifies the
// dispatcher surfaces auxiliary input bindings (beyond the primary trigger) on
// mc.BindingInput, so a middleware can read them — the seam the durable client
// binding uses to discover the host's durable gRPC endpoint — while the
// trigger payload still flows through mc.InputBytes.
func TestHandleInvocationRequest_BindingInputsAvailableToMiddleware(t *testing.T) {
	disp := newTestDispatcher("req-bind")

	rf := loadFunc(t, disp, "BindingCapture", func(ctx context.Context, _ bindings.TimerInfo) error {
		return nil
	})
	// Attach a durableClient-style auxiliary input binding to the loaded
	// function so the dispatcher surfaces its InputData.
	val, _ := disp.LoadedFunctions.Load(rf.FuncId)
	lf := val.(*LoadedFunction)
	lf.Function.RawBindings = append(lf.Function.RawBindings, bindings.Binding{
		Name:      "durableClient",
		Type:      "durableClient",
		Direction: "in",
	})
	disp.LoadedFunctions.Store(rf.FuncId, lf)

	const triggerPayload = "timer-payload"
	const clientPayload = `{"rpcBaseUrl":"http://127.0.0.1:4001/"}`
	var trigger, aux string
	var auxOK bool
	disp.App.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
			trigger = string(mc.InputBytes())
			b, ok := mc.BindingInput("durableClient")
			aux, auxOK = string(b), ok
			return next(ctx, mc)
		}
	}))

	req := invokeRequest(rf.FuncId, "inv-bind")
	req.InputData = []*pb.ParameterBinding{
		{Name: "timer", RpcData: &pb.ParameterBinding_Data{Data: &pb.TypedData{Data: &pb.TypedData_String_{String_: triggerPayload}}}},
		{Name: "durableClient", RpcData: &pb.ParameterBinding_Data{Data: &pb.TypedData{Data: &pb.TypedData_String_{String_: clientPayload}}}},
	}

	if _, err := handleInvocationRequest(req, disp, "req-bind"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if trigger != triggerPayload {
		t.Errorf("mc.InputBytes() = %q, want %q (trigger payload)", trigger, triggerPayload)
	}
	if !auxOK || aux != clientPayload {
		t.Errorf("mc.BindingInput(durableClient) = (%q, %v), want (%q, true)", aux, auxOK, clientPayload)
	}
}
