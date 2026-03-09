package worker

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"testing"

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
	got := v.Bytes()
	if string(got) != "binary data" {
		t.Errorf("expected %q, got %q", "binary data", string(got))
	}
}

func TestDecodeProto_BytesFromString(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_String_{String_: "text as bytes"}}
	v, err := decodeProto(data, reflect.TypeOf([]byte{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := v.Bytes()
	if string(got) != "text as bytes" {
		t.Errorf("expected %q, got %q", "text as bytes", string(got))
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
	v, err := decodeProto(data, reflect.TypeOf(float64(0)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.Float() != 3.14 {
		t.Errorf("expected 3.14, got %f", v.Float())
	}
}

func TestDecodeProto_JSON(t *testing.T) {
	type MyStruct struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	jsonData := `{"name":"Alice","age":30}`
	data := &pb.TypedData{Data: &pb.TypedData_Json{Json: jsonData}}
	v, err := decodeProto(data, reflect.TypeOf(MyStruct{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := v.Interface().(MyStruct)
	if result.Name != "Alice" {
		t.Errorf("expected name %q, got %q", "Alice", result.Name)
	}
	if result.Age != 30 {
		t.Errorf("expected age 30, got %d", result.Age)
	}
}

func TestDecodeProto_HttpRequest(t *testing.T) {
	data := &pb.TypedData{
		Data: &pb.TypedData_Http{
			Http: &pb.RpcHttp{
				Method: "POST",
				Url:    "http://localhost/api/test",
				Headers: map[string]string{
					"Content-Type": "application/json",
				},
				Body: &pb.TypedData{
					Data: &pb.TypedData_String_{String_: `{"key":"value"}`},
				},
			},
		},
	}

	v, err := decodeProto(data, reflect.TypeOf(http.Request{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	req := v.Interface().(http.Request)
	if req.Method != "POST" {
		t.Errorf("expected method POST, got %s", req.Method)
	}
}

func TestDecodeProto_NilData(t *testing.T) {
	v, err := decodeProto(nil, reflect.TypeOf(""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.String() != "" {
		t.Errorf("expected zero value for nil data, got %q", v.String())
	}
}

func TestDecodeProto_BytesDataWithStringTarget(t *testing.T) {
	// When target type is string, the switch case returns data.GetString_() immediately.
	// Since the data is in Bytes (not String_), GetString_() returns "".
	// The later "Bytes handling" section is never reached.
	data := &pb.TypedData{Data: &pb.TypedData_Bytes{Bytes: []byte("byte content")}}
	v, err := decodeProto(data, reflect.TypeOf(""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Due to early return in switch, bytes data is not converted to string
	if v.String() != "" {
		t.Errorf("expected empty string (early return), got %q", v.String())
	}
}

func TestDecodeProto_JSONDataWithBytesTarget(t *testing.T) {
	// When target is []byte, the switch case for Slice/Uint8 runs first.
	// It checks GetBytes() (empty) and GetString_() (empty), then returns GetBytes() = nil.
	// The later JSON fallback and Json handling sections are never reached.
	data := &pb.TypedData{Data: &pb.TypedData_Json{Json: `{"key":"value"}`}}
	v, err := decodeProto(data, reflect.TypeOf([]byte{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Due to early return in switch, JSON data is not converted to []byte
	if v.Len() != 0 {
		t.Errorf("expected empty bytes (early return), got %d bytes", v.Len())
	}
}

// --- encodeProto tests ---

func TestEncodeProto_JSON(t *testing.T) {
	type MyResult struct {
		Message string `json:"message"`
	}
	v := reflect.ValueOf(MyResult{Message: "hello"})
	td, err := encodeProto(v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	jsonStr := td.GetJson()
	if jsonStr == "" {
		t.Fatal("expected JSON data")
	}

	var result MyResult
	if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}
	if result.Message != "hello" {
		t.Errorf("expected message %q, got %q", "hello", result.Message)
	}
}

func TestEncodeProto_String(t *testing.T) {
	v := reflect.ValueOf("simple string")
	td, err := encodeProto(v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	jsonStr := td.GetJson()
	if jsonStr == "" {
		t.Fatal("expected JSON data")
	}
	// Strings get JSON-encoded as quoted strings
	if !strings.Contains(jsonStr, "simple string") {
		t.Errorf("expected JSON to contain %q, got %q", "simple string", jsonStr)
	}
}

func TestEncodeProto_HttpResponse(t *testing.T) {
	resp := &http.Response{
		StatusCode: 201,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       ioutil.NopCloser(strings.NewReader(`{"created":true}`)),
	}

	v := reflect.ValueOf(resp).Elem()
	td, err := encodeProto(v)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	httpData := td.GetHttp()
	if httpData == nil {
		t.Fatal("expected HTTP typed data")
	}
	if httpData.StatusCode != "201" {
		t.Errorf("expected status %q, got %q", "201", httpData.StatusCode)
	}
	if httpData.Headers["Content-Type"] != "application/json" {
		t.Errorf("expected Content-Type header %q, got %q", "application/json", httpData.Headers["Content-Type"])
	}
}

func TestEncodeProto_Nil(t *testing.T) {
	td, err := encodeProto(reflect.Value{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if td != nil {
		t.Errorf("expected nil for invalid value, got %+v", td)
	}
}

func TestEncodeProto_NilPointer(t *testing.T) {
	var p *string
	td, err := encodeProto(reflect.ValueOf(p))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if td != nil {
		t.Errorf("expected nil for nil pointer, got %+v", td)
	}
}

// --- encodeHTTP tests ---

func TestEncodeHTTP(t *testing.T) {
	resp := &http.Response{
		StatusCode: 200,
		Header: http.Header{
			"Content-Type":  {"text/plain"},
			"X-Custom":      {"custom-value"},
		},
		Body: ioutil.NopCloser(strings.NewReader("response body")),
	}

	rpcHttp, err := encodeHTTP(resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if rpcHttp.StatusCode != "200" {
		t.Errorf("expected status %q, got %q", "200", rpcHttp.StatusCode)
	}
	if rpcHttp.Headers["Content-Type"] != "text/plain" {
		t.Errorf("expected Content-Type %q, got %q", "text/plain", rpcHttp.Headers["Content-Type"])
	}
	if rpcHttp.Headers["X-Custom"] != "custom-value" {
		t.Errorf("expected X-Custom %q, got %q", "custom-value", rpcHttp.Headers["X-Custom"])
	}
	if rpcHttp.Body == nil {
		t.Fatal("expected body")
	}
	bodyBytes := rpcHttp.Body.GetBytes()
	if string(bodyBytes) != "response body" {
		t.Errorf("expected body %q, got %q", "response body", string(bodyBytes))
	}
}

func TestEncodeHTTP_NilResponse(t *testing.T) {
	rpcHttp, err := encodeHTTP(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rpcHttp != nil {
		t.Errorf("expected nil for nil response, got %+v", rpcHttp)
	}
}

func TestEncodeHTTP_NilBody(t *testing.T) {
	resp := &http.Response{
		StatusCode: 204,
		Header:     http.Header{},
		Body:       nil,
	}

	rpcHttp, err := encodeHTTP(resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rpcHttp.StatusCode != "204" {
		t.Errorf("expected status %q, got %q", "204", rpcHttp.StatusCode)
	}
	if rpcHttp.Body != nil {
		t.Errorf("expected nil body, got %+v", rpcHttp.Body)
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
		t.Errorf("expected original value to be preserved, got %q", args[0].String())
	}
}

func TestFromProto_UnknownInput(t *testing.T) {
	fields := map[string]*funcField{
		"knownInput": {
			Name:       "knownInput",
			Type:       reflect.TypeOf(""),
			Position:   0,
			Direction:  "in",
			IsArgument: true,
		},
	}

	args := make([]reflect.Value, 1)
	args[0] = reflect.ValueOf("default")

	req := &pb.InvocationRequest{
		InputData: []*pb.ParameterBinding{
			{
				Name: "unknownInput",
				RpcData: &pb.ParameterBinding_Data{
					Data: &pb.TypedData{
						Data: &pb.TypedData_String_{String_: "data"},
					},
				},
			},
		},
	}

	err := FromProto(req, fields, args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// original value should be preserved since input name doesn't match
	if args[0].String() != "default" {
		t.Errorf("expected default value preserved, got %q", args[0].String())
	}
}

// --- ToProto tests ---

func TestToProto_SimpleReturnValue(t *testing.T) {
	results := []reflect.Value{reflect.ValueOf("result value")}
	fields := map[string]*funcField{}

	outputData, returnValue, status, err := ToProto(nil, results, fields)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(outputData) != 0 {
		t.Errorf("expected no output data, got %d", len(outputData))
	}
	if returnValue == nil {
		t.Fatal("expected return value")
	}
	if status.Status != pb.StatusResult_Success {
		t.Errorf("expected Success, got %v", status.Status)
	}
}

func TestToProto_ErrorReturnValue(t *testing.T) {
	results := []reflect.Value{
		reflect.ValueOf(""),
		reflect.ValueOf(json.Unmarshal([]byte("invalid"), nil)), // produces an error
	}
	fields := map[string]*funcField{}

	_, _, status, _ := ToProto(nil, results, fields)

	if status.Status != pb.StatusResult_Failure {
		t.Errorf("expected Failure, got %v", status.Status)
	}
	if status.Exception == nil {
		t.Fatal("expected exception")
	}
}

func TestToProto_WriterOutput(t *testing.T) {
	proxy := NewResponseWriterProxy()
	proxy.Header().Set("Content-Type", "text/plain")
	proxy.WriteHeader(http.StatusOK)
	proxy.Write([]byte("hello from writer"))

	args := []reflect.Value{reflect.ValueOf(proxy)}
	results := []reflect.Value{}

	fields := map[string]*funcField{
		"$return": {
			Name:       "$return",
			Direction:  "out",
			IsArgument: true,
			IsWriter:   true,
			Position:   0,
		},
	}

	_, returnValue, status, err := ToProto(args, results, fields)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status.Status != pb.StatusResult_Success {
		t.Errorf("expected Success, got %v", status.Status)
	}
	if returnValue == nil {
		t.Fatal("expected return value from writer")
	}

	httpData := returnValue.GetHttp()
	if httpData == nil {
		t.Fatal("expected HTTP return data")
	}
	if httpData.StatusCode != "200" {
		t.Errorf("expected status %q, got %q", "200", httpData.StatusCode)
	}
}

// --- convertToTypeValue tests ---

func TestConvertToTypeValue_StringType(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_String_{String_: "hello"}}
	v, err := convertToTypeValue(reflect.TypeOf(""), data, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.String() != "hello" {
		t.Errorf("expected %q, got %q", "hello", v.String())
	}
}

func TestConvertToTypeValue_PointerType(t *testing.T) {
	type MyData struct {
		Value string `json:"value"`
	}

	data := &pb.TypedData{Data: &pb.TypedData_Json{Json: `{"value":"test"}`}}
	v, err := convertToTypeValue(reflect.TypeOf((*MyData)(nil)), data, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if v.Kind() != reflect.Ptr {
		t.Fatalf("expected pointer type, got %v", v.Kind())
	}

	result := v.Interface().(*MyData)
	if result.Value != "test" {
		t.Errorf("expected value %q, got %q", "test", result.Value)
	}
}

func TestConvertToTypeValue_StructWithTriggerMetadata(t *testing.T) {
	type EventData struct {
		Subject string `json:"subject"`
		Topic   string `json:"topic"`
	}

	data := &pb.TypedData{Data: &pb.TypedData_String_{String_: ""}}
	tm := map[string]*pb.TypedData{
		"subject": {Data: &pb.TypedData_String_{String_: "test-subject"}},
		"topic":   {Data: &pb.TypedData_String_{String_: "test-topic"}},
	}

	v, err := convertToTypeValue(reflect.TypeOf(EventData{}), data, tm, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result := v.Interface().(EventData)
	if result.Subject != "test-subject" {
		t.Errorf("expected subject %q, got %q", "test-subject", result.Subject)
	}
	if result.Topic != "test-topic" {
		t.Errorf("expected topic %q, got %q", "test-topic", result.Topic)
	}
}

// --- Additional decodeProto edge case tests ---

func TestDecodeProto_StringFromStringData(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_String_{String_: "via fallback"}}
	v, err := decodeProto(data, reflect.TypeOf(""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.String() != "via fallback" {
		t.Errorf("expected %q, got %q", "via fallback", v.String())
	}
}

func TestDecodeProto_ByteSliceFromStringFallback(t *testing.T) {
	// When Bytes is empty but String_ has a value, bytes target gets the string as bytes
	data := &pb.TypedData{Data: &pb.TypedData_String_{String_: "string-to-bytes"}}
	v, err := decodeProto(data, reflect.TypeOf([]byte{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(v.Bytes()) != "string-to-bytes" {
		t.Errorf("expected %q, got %q", "string-to-bytes", string(v.Bytes()))
	}
}

func TestDecodeProto_IntZeroValue(t *testing.T) {
	// Int with 0 value falls through (code checks data.GetInt() != 0)
	data := &pb.TypedData{Data: &pb.TypedData_Int{Int: 0}}
	v, err := decodeProto(data, reflect.TypeOf(0))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.Int() != 0 {
		t.Errorf("expected 0, got %d", v.Int())
	}
}

func TestDecodeProto_DoubleZeroValue(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_Double{Double: 0}}
	v, err := decodeProto(data, reflect.TypeOf(float64(0)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v.Float() != 0 {
		t.Errorf("expected 0, got %f", v.Float())
	}
}

func TestDecodeProto_StructFromJSONFallback(t *testing.T) {
	type Item struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	data := &pb.TypedData{Data: &pb.TypedData_Json{Json: `{"id":42,"name":"widget"}`}}
	v, err := decodeProto(data, reflect.TypeOf(Item{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	item := v.Interface().(Item)
	if item.ID != 42 || item.Name != "widget" {
		t.Errorf("expected {42, widget}, got %+v", item)
	}
}

func TestDecodeProto_StructFromStringJSON(t *testing.T) {
	// Struct from string that contains JSON (string handling section)
	type Info struct {
		Key string `json:"key"`
	}
	data := &pb.TypedData{Data: &pb.TypedData_String_{String_: `{"key":"value"}`}}
	v, err := decodeProto(data, reflect.TypeOf(Info{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	info := v.Interface().(Info)
	if info.Key != "value" {
		t.Errorf("expected key %q, got %q", "value", info.Key)
	}
}

func TestDecodeProto_HttpRequestPointer(t *testing.T) {
	data := &pb.TypedData{
		Data: &pb.TypedData_Http{
			Http: &pb.RpcHttp{
				Method: "DELETE",
				Url:    "http://localhost/api/resource/123",
			},
		},
	}
	v, err := decodeProto(data, reflect.TypeOf(http.Request{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	req := v.Interface().(http.Request)
	if req.Method != "DELETE" {
		t.Errorf("expected method DELETE, got %s", req.Method)
	}
}
