package worker

import (
	"net/http"
	"reflect"
	"strings"
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

// TestDecodeProto_Fallbacks is a table-driven test covering decodeProto's
// fallback paths. The function tries the "natural" TypedData variant for the
// target Go type first (String_ for string, Int for int64, etc.) and falls
// through to JSON/bytes handling when that variant is empty. Different host
// extensions populate different variants — for example the Storage Queue
// extension sends DequeueCount as TypedData_Json and time fields as
// JSON-quoted strings — so these fallbacks must be reliable.
func TestDecodeProto_Fallbacks(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	tests := []struct {
		name      string
		data      *pb.TypedData
		target    interface{}
		expected  interface{}
		expectErr bool
	}{
		// --- string target ---
		{
			name:     "string from TypedData_String_ (direct, preferred)",
			data:     &pb.TypedData{Data: &pb.TypedData_String_{String_: "direct"}},
			target:   "",
			expected: "direct",
		},
		{
			name:     "string from TypedData_Json (queue ExpirationTime scenario)",
			data:     &pb.TypedData{Data: &pb.TypedData_Json{Json: `"2026-06-29T19:04:34+00:00"`}},
			target:   "",
			expected: "2026-06-29T19:04:34+00:00",
		},
		{
			name:     "string from TypedData_Bytes",
			data:     &pb.TypedData{Data: &pb.TypedData_Bytes{Bytes: []byte("from bytes")}},
			target:   "",
			expected: "from bytes",
		},
		{
			name:     "empty TypedData_String_ yields zero value",
			data:     &pb.TypedData{Data: &pb.TypedData_String_{String_: ""}},
			target:   "",
			expected: "",
		},

		// --- int target ---
		{
			name:     "int64 from TypedData_Json (queue DequeueCount scenario)",
			data:     &pb.TypedData{Data: &pb.TypedData_Json{Json: `1`}},
			target:   int64(0),
			expected: int64(1),
		},
		{
			name:     "int from TypedData_Json",
			data:     &pb.TypedData{Data: &pb.TypedData_Json{Json: `7`}},
			target:   int(0),
			expected: int(7),
		},

		// --- float target ---
		{
			name:     "float64 from TypedData_Json",
			data:     &pb.TypedData{Data: &pb.TypedData_Json{Json: `2.718`}},
			target:   float64(0),
			expected: float64(2.718),
		},

		// --- struct target ---
		{
			name:     "struct from TypedData_String_ containing JSON",
			data:     &pb.TypedData{Data: &pb.TypedData_String_{String_: `{"name":"Alice","age":30}`}},
			target:   payload{},
			expected: payload{Name: "Alice", Age: 30},
		},
		{
			name:      "struct from malformed TypedData_Json returns error",
			data:      &pb.TypedData{Data: &pb.TypedData_Json{Json: `{invalid`}},
			target:    payload{},
			expectErr: true,
		},

		// --- []byte / json.RawMessage target (e.g. QueueMessage.Body) ---
		{
			name:     "[]byte from TypedData_Bytes",
			data:     &pb.TypedData{Data: &pb.TypedData_Bytes{Bytes: []byte("raw bytes")}},
			target:   []byte(nil),
			expected: []byte("raw bytes"),
		},
		{
			name:     "[]byte from TypedData_String_",
			data:     &pb.TypedData{Data: &pb.TypedData_String_{String_: "string body"}},
			target:   []byte(nil),
			expected: []byte("string body"),
		},
		{
			name:     "[]byte from TypedData_Json (queue body as JSON variant)",
			data:     &pb.TypedData{Data: &pb.TypedData_Json{Json: `{"k":"v"}`}},
			target:   []byte(nil),
			expected: []byte(`{"k":"v"}`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v, err := decodeProto(tc.data, reflect.TypeOf(tc.target))
			if tc.expectErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got := v.Interface()
			if !reflect.DeepEqual(got, tc.expected) {
				t.Errorf("expected %v (%T), got %v (%T)", tc.expected, tc.expected, got, got)
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

// --- SQL change-slice decoding tests ---

const sqlChangesJSON = `[` +
	`{"Operation":0,"Item":{"ProductId":1,"Name":"Widget","Cost":100}},` +
	`{"Operation":1,"Item":{"ProductId":2,"Name":"Gadget","Cost":250}},` +
	`{"Operation":2,"Item":{"ProductId":3,"Name":"Gizmo","Cost":50}}` +
	`]`

// TestConvertToTypeValue_SQLChanges_FromJSON locks down that the
// converter decodes the wire payload into a typed []bindings.SQLChange
// when the host packages it as TypedData_Json.
func TestConvertToTypeValue_SQLChanges_FromJSON(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_Json{Json: sqlChangesJSON}}
	t.Logf("payload: %s", data.GetJson())

	v, err := convertToTypeValue(reflect.TypeOf([]bindings.SQLChange{}), data, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	changes, ok := v.Interface().([]bindings.SQLChange)
	if !ok {
		t.Fatalf("expected []bindings.SQLChange, got %T", v.Interface())
	}
	if len(changes) != 3 {
		t.Fatalf("expected 3 changes, got %d", len(changes))
	}
	wantOps := []bindings.SQLOperation{
		bindings.SQLOperationInsert,
		bindings.SQLOperationUpdate,
		bindings.SQLOperationDelete,
	}
	for i, want := range wantOps {
		if changes[i].Operation != want {
			t.Errorf("change[%d]: expected %v, got %v", i, want, changes[i].Operation)
		}
		if len(changes[i].Item) == 0 {
			t.Errorf("change[%d]: expected non-empty Item RawMessage", i)
		}
	}
}

// TestConvertToTypeValue_SQLChanges_FromString covers the alternative
// wire format where the host packages the payload as TypedData_String_
// rather than TypedData_Json. Older host versions and some out-of-process
// integrations have been observed to use String_ for JSON payloads.
func TestConvertToTypeValue_SQLChanges_FromString(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_String_{String_: sqlChangesJSON}}

	v, err := convertToTypeValue(reflect.TypeOf([]bindings.SQLChange{}), data, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	changes, ok := v.Interface().([]bindings.SQLChange)
	if !ok {
		t.Fatalf("expected []bindings.SQLChange, got %T", v.Interface())
	}
	if len(changes) != 3 {
		t.Fatalf("expected 3 changes, got %d", len(changes))
	}
	if changes[0].Operation != bindings.SQLOperationInsert {
		t.Errorf("change[0]: expected Insert, got %v", changes[0].Operation)
	}
}

// TestConvertToTypeValue_SQLChanges_EmptyBatch confirms that a legitimate
// empty change batch (the host can deliver one on polling cycles with no
// activity but a forced flush) decodes to an empty slice without error.
func TestConvertToTypeValue_SQLChanges_EmptyBatch(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_Json{Json: `[]`}}

	v, err := convertToTypeValue(reflect.TypeOf([]bindings.SQLChange{}), data, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	changes, ok := v.Interface().([]bindings.SQLChange)
	if !ok {
		t.Fatalf("expected []bindings.SQLChange, got %T", v.Interface())
	}
	if len(changes) != 0 {
		t.Errorf("expected empty batch, got %d changes", len(changes))
	}
}

// TestConvertToTypeValue_SQLChanges_MalformedStringJSON_ReturnsError locks
// down that a String_-packaged payload targeted at a slice/map but containing
// malformed JSON surfaces an explicit decode error rather than silently
// degrading to an empty slice. Without this guarantee a corrupted batch
// would be indistinguishable from the legitimate empty-batch case above.
func TestConvertToTypeValue_SQLChanges_MalformedStringJSON_ReturnsError(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_String_{String_: `[{"Operation":0,`}} // truncated

	_, err := convertToTypeValue(reflect.TypeOf([]bindings.SQLChange{}), data, nil)
	if err == nil {
		t.Fatal("expected decode error for truncated JSON, got nil")
	}
	// Must mention the target type so the operator can tell which binding
	// failed when several appear in one request.
	if !strings.Contains(err.Error(), "SQLChange") {
		t.Errorf("error should name the target type; got %q", err.Error())
	}
}

// TestConvertToTypeValue_MapStringInt_FromMalformedString_ReturnsError is
// the map-target analogue of the slice case — same contract, different
// reflect.Kind path through the new branch.
func TestConvertToTypeValue_MapStringInt_FromMalformedString_ReturnsError(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_String_{String_: `{"a":1,`}} // truncated

	_, err := convertToTypeValue(reflect.TypeOf(map[string]int{}), data, nil)
	if err == nil {
		t.Fatal("expected decode error for truncated JSON map payload, got nil")
	}
}
