package worker

import (
	"context"
	"encoding/json"
	"math/big"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
	"google.golang.org/protobuf/proto"
)

type pointerText string

func (*pointerText) MarshalText() ([]byte, error) { return []byte("custom"), nil }

// Named pointer types have no methods, but encoding/json still uses the
// pointer methods of the addressable value they point to.
type pointerTextRef *pointerText
type stringRef *string

// TestEncodeReturnValueFollowsPointers checks custom marshalers first, then
// follows non-nil pointers and interfaces to the same wire kinds as values.
func TestEncodeReturnValueFollowsPointers(t *testing.T) {
	text, binary, number := "hello", []byte{0, 255}, json.Number("9007199254740993")
	textPointer, named, namedBinary := &text, genericText("named"), genericBinary{0, 255}
	var dynamic any = text
	var typedNil json.Marshaler = (*big.Int)(nil)
	custom, ip := pointerText("raw"), net.IPv4(127, 0, 0, 1)
	str := func(s string) *pb.TypedData { return &pb.TypedData{Data: &pb.TypedData_String_{String_: s}} }
	raw := func(b []byte) *pb.TypedData { return &pb.TypedData{Data: &pb.TypedData_Bytes{Bytes: b}} }
	js := func(s string) *pb.TypedData { return &pb.TypedData{Data: &pb.TypedData_Json{Json: s}} }
	for _, tt := range []struct {
		name  string
		value any
		want  *pb.TypedData
	}{
		{"string pointer", &text, str("hello")},
		{"bytes pointer", &binary, raw([]byte{0, 255})},
		{"double pointer", &textPointer, str("hello")},
		{"named text pointer", &named, str("named")},
		{"named bytes pointer", &namedBinary, raw([]byte{0, 255})},
		{"interface pointer", &dynamic, str("hello")},
		{"number pointer", &number, js("9007199254740993")},
		{"text marshaler pointer", &ip, js(`"127.0.0.1"`)},
		{"pointer receiver marshaler", &custom, js(`"custom"`)},
		// Like encoding/json, a pointer-receiver method does not apply to a value.
		{"pointer receiver marshaler value", custom, str("raw")},
		// Values reached through a pointer stay addressable for encoding/json.
		{"pointer receiver field", &struct{ Amount big.Int }{*big.NewInt(12345)}, js(`{"Amount":12345}`)},
		{"pointer receiver array element", &[1]pointerText{"raw"}, js(`["custom"]`)},
		{"named pointer marshaler", pointerTextRef(&custom), js(`"custom"`)},
		{"named pointer text", stringRef(&text), str("hello")},
		{"nil pointer", (*string)(nil), nil},
		{"nil marshaler pointer", (*net.IP)(nil), nil},
		{"nil nested pointer", new(*string), nil},
		{"nil interface pointer", new(any), nil},
		{"nil inside marshaler interface", &typedNil, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := encodeReturnValue(tt.value)
			if err != nil || !proto.Equal(got, tt.want) {
				t.Fatalf("got %v (error %v), want %v", got, err, tt.want)
			}
		})
	}
}

func TestEncodeReturnValueStopsAtPointerCycles(t *testing.T) {
	var cyclic any
	cyclic = &cyclic
	done := make(chan error, 1)
	go func() {
		_, err := encodeReturnValue(cyclic)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "cycle") {
			t.Fatalf("error = %v, want cycle error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("encoding a cyclic pointer did not terminate")
	}
}

// Protobuf string fields must be valid UTF-8. Invalid text fails the
// invocation instead of producing a response the gRPC stream cannot send.
func TestEncodeReturnValueRejectsInvalidUTF8Text(t *testing.T) {
	text := "ok\xff"
	for _, value := range []any{text, &text, genericText(text), json.RawMessage(`"` + text + `"`)} {
		if got, err := encodeReturnValue(value); err == nil || !strings.Contains(err.Error(), "UTF-8") {
			t.Fatalf("%T: got %v, error %v, want UTF-8 error", value, got, err)
		}
	}
	disp, rf := loadGeneric(t, func(context.Context, []byte) (*string, error) { return &text, nil })
	resp, err := handleInvocationRequest(genericRequest(rf, &pb.TypedData{Data: &pb.TypedData_String_{String_: "input"}}), disp, "request")
	if err != nil || resp.GetInvocationResponse().GetResult().GetStatus() != pb.StatusResult_Failure {
		t.Fatalf("%v %v", resp, err)
	}
	if _, err := proto.Marshal(resp); err != nil {
		t.Fatalf("response cannot be sent: %v", err)
	}
}

func TestPointerResultsReachHostAsValues(t *testing.T) {
	input := &pb.TypedData{Data: &pb.TypedData_String_{String_: "input"}}
	disp, rf := loadGeneric(t, func(context.Context, []byte) (*[]byte, error) { b := []byte{0, 255}; return &b, nil })
	resp, err := handleInvocationRequest(genericRequest(rf, input), disp, "request")
	if err != nil || !proto.Equal(resp.GetInvocationResponse().GetReturnValue(), &pb.TypedData{Data: &pb.TypedData_Bytes{Bytes: []byte{0, 255}}}) {
		t.Fatalf("bytes pointer: %v %v", resp, err)
	}
	disp, rf = loadGeneric(t, func(context.Context, []byte) (*genericOrder, error) { return nil, nil })
	resp, err = handleInvocationRequest(genericRequest(rf, input), disp, "request")
	if err != nil || resp.GetInvocationResponse().GetResult().GetStatus() != pb.StatusResult_Success || resp.GetInvocationResponse().GetReturnValue() != nil {
		t.Fatalf("nil pointer must send no return value: %v %v", resp, err)
	}
}

// TestEncodeReturnValue covers the encoding the dispatcher applies to a
// non-HTTP function's return value (or a value a middleware records via
// mc.SetReturnValue): strings and bytes pass through as the matching
// TypedData kind; everything else is JSON-encoded; nil yields no TypedData.
func TestEncodeReturnValue(t *testing.T) {
	encode := func(value any) *pb.TypedData {
		t.Helper()
		data, err := encodeReturnValue(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	if td := encode(nil); td != nil {
		t.Errorf("nil → %v, want nil", td)
	}
	if td := encode("hi"); td.GetString_() != "hi" {
		t.Errorf("string → %q, want hi", td.GetString_())
	}
	if td := encode([]byte("bytes")); string(td.GetBytes()) != "bytes" {
		t.Errorf("[]byte → %q, want bytes", td.GetBytes())
	}
	type payload struct {
		A int `json:"a"`
	}
	if td := encode(payload{A: 7}); td.GetJson() != `{"a":7}` {
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
