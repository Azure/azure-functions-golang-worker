package worker

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

type genericByte uint8
type genericBinary []genericByte
type panickingJSON struct{}

func (*panickingJSON) UnmarshalJSON([]byte) error  { panic("bad custom decoder") }
func (panickingJSON) MarshalJSON() ([]byte, error) { panic("bad custom encoder") }

func TestGenericTriggerNamedBytes(t *testing.T) {
	want := genericBinary{0, 255, 12}
	var got genericBinary
	disp, rf := loadGeneric(t, func(_ context.Context, value genericBinary) error {
		got = value
		return nil
	})
	resp, err := handleInvocationRequest(genericRequest(rf, &pb.TypedData{Data: &pb.TypedData_Bytes{Bytes: []byte{0, 255, 12}}}), disp, "request")
	if err != nil || resp.GetInvocationResponse().GetResult().GetStatus() != pb.StatusResult_Success || !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v response=%v error=%v", got, resp, err)
	}
}

func TestGenericTriggerCustomDecoderPanicFailsInvocation(t *testing.T) {
	called := false
	disp, rf := loadGeneric(t, func(context.Context, panickingJSON) error { called = true; return nil })
	resp, err := handleInvocationRequest(genericRequest(rf, &pb.TypedData{Data: &pb.TypedData_Json{Json: `{}`}}), disp, "request")
	if err != nil || called || resp.GetInvocationResponse().GetResult().GetStatus() != pb.StatusResult_Failure {
		t.Fatalf("called=%v response=%v error=%v", called, resp, err)
	}
	if !strings.Contains(resp.GetInvocationResponse().GetResult().GetException().GetMessage(), "bad custom decoder") {
		t.Fatal(resp)
	}
}

func TestGenericTriggerReturnContracts(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value any
		want  *pb.TypedData
	}{
		{"named text", genericText("hello"), &pb.TypedData{Data: &pb.TypedData_String_{String_: "hello"}}},
		{"named binary", genericBinary{0, 255}, &pb.TypedData{Data: &pb.TypedData_Bytes{Bytes: []byte{0, 255}}}},
		{"JSON marshaler", json.RawMessage(`{"id":"one"}`), &pb.TypedData{Data: &pb.TypedData_Json{Json: `{"id":"one"}`}}},
		{"batch result", []genericOrder{{ID: "one"}}, &pb.TypedData{Data: &pb.TypedData_Json{Json: `[{"id":"one"}]`}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			disp, rf := loadGeneric(t, func(context.Context, []byte) (any, error) { return tt.value, nil })
			resp, err := handleInvocationRequest(genericRequest(rf, &pb.TypedData{Data: &pb.TypedData_String_{String_: "input"}}), disp, "request")
			if err != nil || resp.GetInvocationResponse().GetResult().GetStatus() != pb.StatusResult_Success || !reflect.DeepEqual(resp.GetInvocationResponse().GetReturnValue(), tt.want) {
				t.Fatalf("response=%v error=%v, want=%v", resp, err, tt.want)
			}
		})
	}
}

func TestGenericTriggerErrorRetainsTracing(t *testing.T) {
	for _, handlerError := range []error{nil, errors.New("handler failed")} {
		t.Run(strings.ReplaceAll("error-"+errorText(handlerError), " ", "-"), func(t *testing.T) {
			disp, rf := loadGeneric(t, func(context.Context, []byte) (any, error) { return panickingJSON{}, handlerError })
			disp.App.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
				return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
					mc.SetOutboundTraceAttribute("test", "preserved")
					return next(ctx, mc)
				}
			}))
			resp, err := handleInvocationRequest(genericRequest(rf, &pb.TypedData{Data: &pb.TypedData_String_{String_: "input"}}), disp, "request")
			if err != nil || resp.GetInvocationResponse().GetResult().GetStatus() != pb.StatusResult_Failure {
				t.Fatalf("%v %v", resp, err)
			}
			want := "bad custom encoder"
			if handlerError != nil {
				want = handlerError.Error()
			}
			if !strings.Contains(resp.GetInvocationResponse().GetResult().GetException().GetMessage(), want) || resp.GetInvocationResponse().TraceContextAttributes["test"] != "preserved" {
				t.Fatal(resp)
			}
		})
	}
}

func errorText(err error) string {
	if err == nil {
		return "nil"
	}
	return err.Error()
}

func TestGenericAndTypedTriggersShareDispatcher(t *testing.T) {
	disp := newTestDispatcher("request")
	var got []genericOrder
	var timerCalled bool
	grf := disp.App.GenericTrigger("Batch", func(_ context.Context, values []genericOrder) ([]genericOrder, error) {
		got = values
		return values, nil
	}, &bindings.GenericTrigger{Type: "unfamiliarTrigger", Name: "event", Cardinality: "many"})
	trf := disp.App.Timer("Timer", func(context.Context, bindings.TimerInfo) error { timerCalled = true; return nil })
	for _, rf := range []*sdk.RegisteredFunction{grf, trf} {
		handleFunctionLoadRequest(&pb.FunctionLoadRequest{FunctionId: rf.FuncId}, disp, "request")
	}
	resp, err := handleInvocationRequest(genericRequest(grf, &pb.TypedData{Data: &pb.TypedData_CollectionString{CollectionString: &pb.CollectionString{String_: []string{`{"id":"one"}`, `{"id":"two"}`}}}}), disp, "request")
	if err != nil || resp.GetInvocationResponse().GetReturnValue().GetJson() != `[{"id":"one"},{"id":"two"}]` || !reflect.DeepEqual(got, []genericOrder{{"one"}, {"two"}}) {
		t.Fatalf("%v %v %v", got, resp, err)
	}
	resp, err = handleInvocationRequest(invokeRequest(trf.FuncId, "timer"), disp, "request")
	if err != nil || !timerCalled || resp.GetInvocationResponse().GetResult().GetStatus() != pb.StatusResult_Success {
		t.Fatalf("%v %v", resp, err)
	}
}
