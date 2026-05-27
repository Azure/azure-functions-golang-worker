package worker

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// --- decodeProto tests ---

func TestDecodeProto_String(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_String_{String_: "hello"}}
	v, err := decodeProto(data, reflect.TypeOf(""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.String() != "hello" {
		t.Errorf("expected %q, got %q", "hello", v.String())
	}
}

func TestDecodeProto_Bytes(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_Bytes{Bytes: []byte("binary data")}}
	v, err := decodeProto(data, reflect.TypeOf([]byte{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(v.Bytes()) != "binary data" {
		t.Errorf("expected %q, got %q", "binary data", string(v.Bytes()))
	}
}

func TestDecodeProto_BytesFromString(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_String_{String_: "text as bytes"}}
	v, err := decodeProto(data, reflect.TypeOf([]byte{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(v.Bytes()) != "text as bytes" {
		t.Errorf("expected %q, got %q", "text as bytes", string(v.Bytes()))
	}
}

func TestDecodeProto_Int(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_Int{Int: 42}}
	v, err := decodeProto(data, reflect.TypeOf(0))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.Int() != 42 {
		t.Errorf("expected 42, got %d", v.Int())
	}
}

func TestDecodeProto_Float64(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_Double{Double: 3.14}}
	v, err := decodeProto(data, reflect.TypeOf(0.0))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.Float() != 3.14 {
		t.Errorf("expected 3.14, got %f", v.Float())
	}
}

func TestDecodeProto_JSON(t *testing.T) {
	type MyStruct struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}

	data := &pb.TypedData{Data: &pb.TypedData_Json{Json: `{"name":"test","count":5}`}}
	v, err := decodeProto(data, reflect.TypeOf(MyStruct{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	result := v.Interface().(MyStruct)
	if result.Name != "test" {
		t.Errorf("expected name %q, got %q", "test", result.Name)
	}
	if result.Count != 5 {
		t.Errorf("expected count 5, got %d", result.Count)
	}
}

func TestDecodeProto_NilData(t *testing.T) {
	v, err := decodeProto(nil, reflect.TypeOf(""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.String() != "" {
		t.Errorf("expected empty string for nil data, got %q", v.String())
	}
}

func TestDecodeProto_BoolFromString(t *testing.T) {
	tests := []struct {
		name     string
		data     *pb.TypedData
		expected bool
	}{
		{"True string", &pb.TypedData{Data: &pb.TypedData_String_{String_: "True"}}, true},
		{"true string", &pb.TypedData{Data: &pb.TypedData_String_{String_: "true"}}, true},
		{"False string", &pb.TypedData{Data: &pb.TypedData_String_{String_: "False"}}, false},
		{"false string", &pb.TypedData{Data: &pb.TypedData_String_{String_: "false"}}, false},
		{"int nonzero", &pb.TypedData{Data: &pb.TypedData_Int{Int: 1}}, true},
		{"int zero", &pb.TypedData{Data: &pb.TypedData_Int{Int: 0}}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v, err := decodeProto(tc.data, reflect.TypeOf(false))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if v.Bool() != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, v.Bool())
			}
		})
	}
}

// --- FromProto tests ---

func TestFromProto_StringInput(t *testing.T) {
	fields := map[string]*funcField{
		"myInput": {
			Name:       "myInput",
			Type:       reflect.TypeOf(""),
			Position:   0,
			Direction:  "in",
			IsArgument: true,
		},
	}

	args := make([]reflect.Value, 1)
	args[0] = reflect.Zero(reflect.TypeOf(""))

	req := &pb.InvocationRequest{
		InputData: []*pb.ParameterBinding{
			{
				Name: "myInput",
				RpcData: &pb.ParameterBinding_Data{
					Data: &pb.TypedData{
						Data: &pb.TypedData_String_{String_: "test value"},
					},
				},
			},
		},
	}

	err := FromProto(req, fields, args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args[0].String() != "test value" {
		t.Errorf("expected %q, got %q", "test value", args[0].String())
	}
}

func TestFromProto_SkipsOutBindings(t *testing.T) {
	fields := map[string]*funcField{
		"output": {
			Name:       "output",
			Type:       reflect.TypeOf(""),
			Position:   0,
			Direction:  "out",
			IsArgument: true,
		},
	}

	args := make([]reflect.Value, 1)
	args[0] = reflect.ValueOf("original")

	req := &pb.InvocationRequest{
		InputData: []*pb.ParameterBinding{
			{
				Name: "output",
				RpcData: &pb.ParameterBinding_Data{
					Data: &pb.TypedData{
						Data: &pb.TypedData_String_{String_: "should not be set"},
					},
				},
			},
		},
	}

	err := FromProto(req, fields, args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args[0].String() != "original" {
		t.Errorf("expected original value preserved, got %q", args[0].String())
	}
}

// --- convertToTypeValue tests ---

func TestConvertToTypeValue_StringType(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_String_{String_: "hello"}}
	v, err := convertToTypeValue(reflect.TypeOf(""), data, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.String() != "hello" {
		t.Errorf("expected %q, got %q", "hello", v.String())
	}
}

func TestConvertToTypeValue_StructWithTriggerMetadata(t *testing.T) {
	type ServiceBusMsg struct {
		Body          string `json:"azfuncdata"`
		MessageId     string `json:"messageId"`
		DeliveryCount int    `json:"deliveryCount"`
	}

	data := &pb.TypedData{Data: &pb.TypedData_String_{String_: "hello"}}
	metadata := map[string]*pb.TypedData{
		"messageId":     {Data: &pb.TypedData_String_{String_: "msg-123"}},
		"deliveryCount": {Data: &pb.TypedData_Int{Int: 3}},
	}

	v, err := convertToTypeValue(reflect.TypeOf(ServiceBusMsg{}), data, metadata)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := v.Interface().(ServiceBusMsg)
	if result.Body != "hello" {
		t.Errorf("expected body %q, got %q", "hello", result.Body)
	}
	if result.MessageId != "msg-123" {
		t.Errorf("expected messageId %q, got %q", "msg-123", result.MessageId)
	}
}

func TestConvertToTypeValue_TimerInfo_StringJSON(t *testing.T) {
	timerJSON := `{"Schedule":{"AdjustForDST":true},"ScheduleStatus":{"Last":"2026-05-15T17:40:00+00:00","Next":"2026-05-15T17:45:00+00:00","LastUpdated":"2026-05-15T17:40:00+00:00"},"IsPastDue":false}`
	data := &pb.TypedData{Data: &pb.TypedData_String_{String_: timerJSON}}
	metadata := map[string]*pb.TypedData{
		"isPastDue": {Data: &pb.TypedData_String_{String_: "False"}},
	}

	v, err := convertToTypeValue(reflect.TypeOf(bindings.TimerInfo{}), data, metadata)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := v.Interface().(bindings.TimerInfo)

	if result.ScheduleStatus.Last != "2026-05-15T17:40:00+00:00" {
		t.Errorf("expected ScheduleStatus.Last %q, got %q",
			"2026-05-15T17:40:00+00:00", result.ScheduleStatus.Last)
	}
	if result.ScheduleStatus.Next != "2026-05-15T17:45:00+00:00" {
		t.Errorf("expected ScheduleStatus.Next %q, got %q",
			"2026-05-15T17:45:00+00:00", result.ScheduleStatus.Next)
	}
	if result.ScheduleStatus.LastUpdated != "2026-05-15T17:40:00+00:00" {
		t.Errorf("expected ScheduleStatus.LastUpdated %q, got %q",
			"2026-05-15T17:40:00+00:00", result.ScheduleStatus.LastUpdated)
	}
	if result.Schedule.AdjustForDST != true {
		t.Errorf("expected Schedule.AdjustForDST true, got %v", result.Schedule.AdjustForDST)
	}
}

// --- encodeHTTPResponse tests ---

func TestEncodeHTTPResponse(t *testing.T) {
	proxy := NewResponseWriterProxy()
	proxy.Header().Set("Content-Type", "text/plain")
	proxy.WriteHeader(http.StatusOK)
	proxy.Write([]byte("hello"))

	td := encodeHTTPResponse(proxy)
	if td == nil {
		t.Fatal("expected non-nil TypedData")
	}

	httpData := td.GetHttp()
	if httpData == nil {
		t.Fatal("expected HTTP data")
	}
	if httpData.StatusCode != "200" {
		t.Errorf("expected status %q, got %q", "200", httpData.StatusCode)
	}
	if httpData.Headers["Content-Type"] != "text/plain" {
		t.Errorf("expected Content-Type %q, got %q", "text/plain", httpData.Headers["Content-Type"])
	}
}
