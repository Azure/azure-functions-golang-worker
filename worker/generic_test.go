package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type genericOrder struct {
	ID string `json:"id"`
}

type genericText string

func TestGenericTriggerPayloads(t *testing.T) {
	tests := []struct {
		name string
		data *pb.TypedData
		want any
	}{
		{"raw JSON", &pb.TypedData{Data: &pb.TypedData_Json{Json: `{"id":"one"}`}}, []byte(`{"id":"one"}`)},
		{"raw JSON string", &pb.TypedData{Data: &pb.TypedData_Json{Json: `"one"`}}, `"one"`},
		{"empty string", &pb.TypedData{Data: &pb.TypedData_String_{String_: ""}}, genericText("")},
		{"named string", &pb.TypedData{Data: &pb.TypedData_String_{String_: "one"}}, genericText("one")},
		{"DTO JSON", &pb.TypedData{Data: &pb.TypedData_Json{Json: `{"id":"one"}`}}, genericOrder{"one"}},
		{"DTO string", &pb.TypedData{Data: &pb.TypedData_String_{String_: `{"id":"one"}`}}, &genericOrder{"one"}},
		{"DTO bytes", &pb.TypedData{Data: &pb.TypedData_Bytes{Bytes: []byte(`{"id":"one"}`)}}, genericOrder{"one"}},
		{"map", &pb.TypedData{Data: &pb.TypedData_String_{String_: `{"id":"one"}`}}, map[string]string{"id": "one"}},
		{"single JSON array", &pb.TypedData{Data: &pb.TypedData_Json{Json: `["one","two"]`}}, []string{"one", "two"}},
		{"integer zero", &pb.TypedData{Data: &pb.TypedData_Int{Int: 0}}, int64(0)},
		{"integer", &pb.TypedData{Data: &pb.TypedData_Int{Int: 7}}, int(7)},
		{"double", &pb.TypedData{Data: &pb.TypedData_Double{Double: 1.5}}, float64(1.5)},
		{"bool JSON", &pb.TypedData{Data: &pb.TypedData_Json{Json: "false"}}, false},
		{"nullable DTO", &pb.TypedData{Data: &pb.TypedData_Json{Json: "null"}}, (*genericOrder)(nil)},
		{"nullable DTO string", &pb.TypedData{Data: &pb.TypedData_String_{String_: "null"}}, (*genericOrder)(nil)},
		{"nullable DTO bytes", &pb.TypedData{Data: &pb.TypedData_Bytes{Bytes: []byte("null")}}, (*genericOrder)(nil)},
		{"raw null", &pb.TypedData{Data: &pb.TypedData_Json{Json: "null"}}, "null"},
		{"batch strings", &pb.TypedData{Data: &pb.TypedData_CollectionString{CollectionString: &pb.CollectionString{String_: []string{"one", "", "three"}}}}, []string{"one", "", "three"}},
		{"batch bytes", &pb.TypedData{Data: &pb.TypedData_CollectionBytes{CollectionBytes: &pb.CollectionBytes{Bytes: [][]byte{[]byte("one"), {}, []byte("three")}}}}, [][]byte{[]byte("one"), {}, []byte("three")}},
		{"batch numbers", &pb.TypedData{Data: &pb.TypedData_CollectionSint64{CollectionSint64: &pb.CollectionSInt64{Sint64: []int64{0, 7}}}}, []int64{0, 7}},
		{"batch doubles", &pb.TypedData{Data: &pb.TypedData_CollectionDouble{CollectionDouble: &pb.CollectionDouble{Double: []float64{0, 1.5}}}}, []float64{0, 1.5}},
		{"batch DTO", &pb.TypedData{Data: &pb.TypedData_CollectionString{CollectionString: &pb.CollectionString{String_: []string{`{"id":"one"}`, `{"id":"two"}`}}}}, []genericOrder{{"one"}, {"two"}}},
		{"empty batch", &pb.TypedData{Data: &pb.TypedData_CollectionString{CollectionString: &pb.CollectionString{}}}, []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got any
			ft := reflect.FuncOf([]reflect.Type{contextType, reflect.TypeOf(tt.want)}, []reflect.Type{errorType}, false)
			fn := reflect.MakeFunc(ft, func(args []reflect.Value) []reflect.Value {
				got = args[1].Interface()
				return []reflect.Value{reflect.Zero(errorType)}
			}).Interface()
			disp, rf := loadGeneric(t, fn)
			resp, err := handleInvocationRequest(genericRequest(rf, tt.data), disp, "request")
			if err != nil || resp.GetInvocationResponse().GetResult().GetStatus() != pb.StatusResult_Success {
				t.Fatalf("invoke = %v, %v", resp, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %#v (%T), want %#v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func loadGeneric(t *testing.T, fn any) (*Dispatcher, *sdk.RegisteredFunction) {
	t.Helper()
	disp := newTestDispatcher("request")
	rf := disp.App.GenericTrigger("Generic", fn, &bindings.GenericTrigger{Type: "unfamiliarTrigger", Name: "event"})
	resp := handleFunctionLoadRequest(&pb.FunctionLoadRequest{FunctionId: rf.FuncId}, disp, "request")
	if resp.GetFunctionLoadResponse().GetResult().GetStatus() != pb.StatusResult_Success {
		t.Fatal(resp)
	}
	return disp, rf
}

func genericRequest(rf *sdk.RegisteredFunction, data *pb.TypedData) *pb.InvocationRequest {
	return &pb.InvocationRequest{InvocationId: "invocation", FunctionId: rf.FuncId, InputData: []*pb.ParameterBinding{
		{Name: "event", RpcData: &pb.ParameterBinding_Data{Data: data}},
	}}
}

func TestGenericTriggerDecodeFailureIsInvocationFailure(t *testing.T) {
	for _, scenario := range []string{"malformed", "missing", "duplicate", "unknown wire kind"} {
		t.Run(scenario, func(t *testing.T) {
			called := false
			disp, rf := loadGeneric(t, func(context.Context, genericOrder) error { called = true; return nil })
			req := genericRequest(rf, &pb.TypedData{Data: &pb.TypedData_String_{String_: "not-json"}})
			switch scenario {
			case "missing":
				req.InputData = nil
			case "duplicate":
				req.InputData[0].RpcData = &pb.ParameterBinding_Data{Data: &pb.TypedData{Data: &pb.TypedData_Json{Json: `{"id":"valid"}`}}}
				req.InputData = append(req.InputData, req.InputData[0])
			case "unknown wire kind":
				req.InputData[0].RpcData = &pb.ParameterBinding_Data{Data: &pb.TypedData{Data: &pb.TypedData_Http{Http: &pb.RpcHttp{}}}}
			}
			resp, err := handleInvocationRequest(req, disp, "request")
			if err != nil {
				t.Fatalf("decode failure must not escape as a stream error: %v", err)
			}
			if called || resp.GetInvocationResponse().GetResult().GetStatus() != pb.StatusResult_Failure {
				t.Fatalf("called=%v response=%v", called, resp)
			}
			if !strings.Contains(resp.GetInvocationResponse().GetResult().GetException().GetMessage(), "event") {
				t.Fatal("missing binding name in error")
			}
			if scenario == "duplicate" && !strings.Contains(resp.GetInvocationResponse().GetResult().GetException().GetMessage(), "duplicate") {
				t.Fatal("expected the duplicate check, not a decode failure")
			}
		})
	}
}

func TestGenericTriggerMetadataAndMiddleware(t *testing.T) {
	var seen *sdk.InvocationContext
	var steps []string
	disp, rf := loadGeneric(t, func(ctx context.Context, _ []byte) (string, error) {
		seen, _ = sdk.FromContext(ctx)
		steps = append(steps, "handler")
		return "done", nil
	})
	disp.App.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
			steps = append(steps, "before")
			err := next(ctx, mc)
			steps = append(steps, "after")
			return err
		}
	}))
	req := genericRequest(rf, &pb.TypedData{Data: &pb.TypedData_Bytes{Bytes: []byte("payload")}})
	req.TriggerMetadata = map[string]*pb.TypedData{
		"empty":      {Data: &pb.TypedData_String_{String_: ""}},
		"sequence":   {Data: &pb.TypedData_Int{Int: 9007199254740993}},
		"properties": {Data: &pb.TypedData_Json{Json: `{"enabled":false}`}},
		"ids":        {Data: &pb.TypedData_CollectionString{CollectionString: &pb.CollectionString{String_: []string{"one", ""}}}},
	}
	resp, err := handleInvocationRequest(req, disp, "request")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(steps, []string{"before", "handler", "after"}) {
		t.Fatal(steps)
	}
	if seen == nil || seen.TriggerMetadataValues["sequence"] != int64(9007199254740993) {
		t.Fatalf("metadata=%+v", seen)
	}
	if value, ok := seen.TriggerMetadataValues["empty"]; !ok || value != "" {
		t.Fatalf("empty=%v,%v", value, ok)
	}
	if string(seen.TriggerMetadataValues["properties"].(json.RawMessage)) != `{"enabled":false}` {
		t.Fatal(seen.TriggerMetadataValues)
	}
	if !reflect.DeepEqual(seen.TriggerMetadataValues["ids"], []string{"one", ""}) {
		t.Fatal(seen.TriggerMetadataValues)
	}
	if resp.GetInvocationResponse().GetReturnValue().GetString_() != "done" {
		t.Fatal(resp)
	}
}

func TestGenericTriggerReturnEncodingErrors(t *testing.T) {
	disp, rf := loadGeneric(t, func(context.Context, []byte) (any, error) { return make(chan int), nil })
	resp, err := handleInvocationRequest(genericRequest(rf, &pb.TypedData{Data: &pb.TypedData_String_{String_: "input"}}), disp, "request")
	if err != nil || resp.GetInvocationResponse().GetResult().GetStatus() != pb.StatusResult_Failure {
		t.Fatalf("%v %v", resp, err)
	}
}

func TestGenericTriggerIndexing(t *testing.T) {
	app := sdk.FunctionApp()
	app.GenericTrigger("Batch", func(context.Context, [][]byte) error { return nil }, &bindings.GenericTrigger{
		Type: "unfamiliarTrigger", Name: "event", DataType: "binary", Cardinality: "many", Properties: map[string]any{"enabled": false},
	})
	resp := handleFunctionsMetadataRequest(&pb.FunctionsMetadataRequest{}, app, "request").GetFunctionMetadataResponse()
	meta := resp.FunctionMetadataResults[0]
	if meta.Bindings["event"].DataType != pb.BindingInfo_binary {
		t.Fatal(meta.Bindings)
	}
	if !strings.Contains(meta.RawBindings[0], `"enabled":false`) || !strings.Contains(meta.RawBindings[0], `"cardinality":"many"`) {
		t.Fatal(meta.RawBindings)
	}
	// Even post-registration mutation must fail indexing rather than omit a binding.
	app.GetRegisteredFunctions().Range(func(_, value any) bool {
		value.(*sdk.RegisteredFunction).RawBindings[0].GenericBinding.Properties["bad"] = make(chan int)
		return true
	})
	failed := handleFunctionsMetadataRequest(&pb.FunctionsMetadataRequest{}, app, "request").GetFunctionMetadataResponse()
	if failed.GetResult().GetStatus() != pb.StatusResult_Failure {
		t.Fatal(fmt.Sprint(failed))
	}
}

// Pin the supported and intentionally unsupported cases to the actual protobuf
// oneof. New protocol fields require an explicit decision rather than silently
// expanding the advertised contract.
func TestGenericTriggerWireKinds(t *testing.T) {
	tests := map[protoreflect.Name]struct {
		data      *pb.TypedData
		target    reflect.Type
		supported bool
	}{
		"string":                        {&pb.TypedData{Data: &pb.TypedData_String_{String_: "text"}}, reflect.TypeFor[string](), true},
		"json":                          {&pb.TypedData{Data: &pb.TypedData_Json{Json: `{"id":"one"}`}}, reflect.TypeFor[genericOrder](), true},
		"bytes":                         {&pb.TypedData{Data: &pb.TypedData_Bytes{Bytes: []byte{}}}, reflect.TypeFor[[]byte](), true},
		"int":                           {&pb.TypedData{Data: &pb.TypedData_Int{}}, reflect.TypeFor[int64](), true},
		"double":                        {&pb.TypedData{Data: &pb.TypedData_Double{}}, reflect.TypeFor[float64](), true},
		"collection_string":             {&pb.TypedData{Data: &pb.TypedData_CollectionString{CollectionString: &pb.CollectionString{}}}, reflect.TypeFor[[]string](), true},
		"collection_bytes":              {&pb.TypedData{Data: &pb.TypedData_CollectionBytes{CollectionBytes: &pb.CollectionBytes{}}}, reflect.TypeFor[[][]byte](), true},
		"collection_sint64":             {&pb.TypedData{Data: &pb.TypedData_CollectionSint64{CollectionSint64: &pb.CollectionSInt64{}}}, reflect.TypeFor[[]int64](), true},
		"collection_double":             {&pb.TypedData{Data: &pb.TypedData_CollectionDouble{CollectionDouble: &pb.CollectionDouble{}}}, reflect.TypeFor[[]float64](), true},
		"stream":                        {&pb.TypedData{Data: &pb.TypedData_Stream{}}, reflect.TypeFor[[]byte](), false},
		"http":                          {&pb.TypedData{Data: &pb.TypedData_Http{Http: &pb.RpcHttp{}}}, reflect.TypeFor[[]byte](), false},
		"model_binding_data":            {&pb.TypedData{Data: &pb.TypedData_ModelBindingData{ModelBindingData: &pb.ModelBindingData{}}}, reflect.TypeFor[[]byte](), false},
		"collection_model_binding_data": {&pb.TypedData{Data: &pb.TypedData_CollectionModelBindingData{CollectionModelBindingData: &pb.CollectionModelBindingData{}}}, reflect.TypeFor[[][]byte](), false},
	}
	fields := (&pb.TypedData{}).ProtoReflect().Descriptor().Oneofs().ByName("data").Fields()
	if len(tests) != fields.Len() {
		t.Fatalf("wire cases=%d, protobuf fields=%d", len(tests), fields.Len())
	}
	for i := 0; i < fields.Len(); i++ {
		name := fields.Get(i).Name()
		t.Run(string(name), func(t *testing.T) {
			tt, ok := tests[name]
			if !ok {
				t.Fatalf("new unclassified wire kind: %s", name)
			}
			_, err := decodeGenericInput(tt.target, tt.data)
			if (err == nil) != tt.supported {
				t.Fatalf("supported=%v, error=%v", tt.supported, err)
			}
			_, preserved := genericTriggerMetadata(map[string]*pb.TypedData{"value": tt.data})["value"]
			if preserved != tt.supported {
				t.Fatalf("metadata preserved=%v, want %v", preserved, tt.supported)
			}
		})
	}
}
