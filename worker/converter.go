package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// funcField represents a representation of a func field
// Used to map binding names to function arguments/return values.
type funcField struct {
	Name       string
	Type       reflect.Type
	Position   int
	Direction  string // "in" or "out"
	IsArgument bool   // true if mapped to function argument, false if return value
	IsWriter   bool   // true if this argument is an http.ResponseWriter
}

// FromProto converts protobuf parameters to golang values
// Only processes "in" bindings mapped to Arguments.
func FromProto(req *pb.InvocationRequest, fields map[string]*funcField, args []reflect.Value) error {
	for _, input := range req.InputData {
		param, ok := fields[input.Name]
		if ok && param.Direction == "in" && param.IsArgument {
			if param.Position < len(args) {
				r, err := convertToTypeValue(param.Type, input.GetData(), req.GetTriggerMetadata())
				if err != nil {
					return err
				}
				args[param.Position] = r
			}
		}
	}
	return nil
}

// azFuncDataTag is the legacy json struct-field tag used by SDK binding
// types to mark the field that receives the raw trigger input body. New
// bindings can preserve their public JSON name with the azfunc:"data" tag.
const azFuncDataTag = "azfuncdata"

// isAzFuncDataField reports whether field f opts into raw trigger input data.
func isAzFuncDataField(f reflect.StructField) bool {
	if strings.EqualFold(f.Tag.Get("azfunc"), "data") {
		return true
	}
	tag := f.Tag.Get("json")
	if idx := strings.Index(tag, ","); idx != -1 {
		tag = tag[:idx]
	}
	return strings.EqualFold(tag, azFuncDataTag)
}

// hasAzFuncDataField reports whether the struct type t declares a field that
// receives raw trigger input. These structs must not be decoded via
// whole-struct json.Unmarshal of the body.
func hasAzFuncDataField(t reflect.Type) bool {
	for i := 0; i < t.NumField(); i++ {
		if isAzFuncDataField(t.Field(i)) {
			return true
		}
	}
	return false
}

// convertToTypeValue returns a native value from protobuf
func convertToTypeValue(pt reflect.Type, data *pb.TypedData, tm map[string]*pb.TypedData) (reflect.Value, error) {
	var t reflect.Type
	if pt.Kind() == reflect.Ptr {
		t = pt.Elem()
	} else {
		t = pt
	}

	pv := reflect.New(t)
	v := pv.Elem()

	// Batch triggers arrive as a TypedData collection plus parallel metadata
	// arrays. Decode each entry as its target struct instead of using the
	// default slice decoder so empty payloads retain their position and each
	// struct receives metadata from the same batch index.
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Struct {
		if values, ok := typedDataCollectionValues(data); ok {
			result := reflect.MakeSlice(t, len(values), len(values))
			for i, value := range values {
				metadata := collectionMetadataAt(tm, i)
				element, err := convertToTypeValue(t.Elem(), value, metadata)
				if err != nil {
					return reflect.Value{}, fmt.Errorf("failed to decode collection element %d: %w", i, err)
				}
				result.Index(i).Set(element)
			}
			return result, nil
		}
	}

	// If struct, try JSON decoding first, then fall back to TriggerMetadata field matching.
	//
	// Skip the whole-struct JSON fast-path when the struct declares an
	// "azfuncdata"-tagged field: that tag signals the input body belongs
	// in that single field (e.g. QueueMessage.Body, ServiceBusMessage.Body),
	// not unmarshalled across the whole struct. Without this guard, a queue
	// message with a JSON object body would silently produce a zero-valued
	// QueueMessage — the unmarshal succeeds but matches no field tags, the
	// fast path returns, and the per-field raw-data + metadata fallback
	// below is never reached.
	if t.Kind() == reflect.Struct && !hasAzFuncDataField(t) {
		// Try direct JSON unmarshal from input data (TypedData_Json or TypedData_String_)
		jsonStr := data.GetJson()
		if jsonStr == "" {
			jsonStr = data.GetString_()
		}
		if jsonStr != "" {
			target := reflect.New(t).Interface()
			if err := json.Unmarshal([]byte(jsonStr), target); err == nil {
				val := reflect.ValueOf(target).Elem()
				if pt.Kind() == reflect.Ptr {
					ptr := reflect.New(t)
					ptr.Elem().Set(val)
					return ptr, nil
				}
				return val, nil
			}
		}
	}

	if t.Kind() == reflect.Struct {
		// Fall back to field-by-field matching from TriggerMetadata
		fieldsDecoded := 0
		for i := 0; i < v.NumField(); i++ {
			field := t.Field(i)

			var td *pb.TypedData
			if isAzFuncDataField(field) {
				td = data
				fieldsDecoded++
			} else {
				tag := field.Tag.Get("json")
				// Strip omitempty or other options from the json tag
				if idx := strings.Index(tag, ","); idx != -1 {
					tag = tag[:idx]
				}
				if val, ok := tm[tag]; ok {
					td = val
					fieldsDecoded++
				} else {
					// Case-insensitive fallback
					for k, val := range tm {
						if strings.EqualFold(k, tag) {
							td = val
							fieldsDecoded++
							break
						}
					}
					if td == nil {
						continue
					}
				}
			}

			if td == nil {
				continue
			}

			d, err := decodeProto(td, field.Type)
			if err != nil {
				return reflect.Value{}, fmt.Errorf("failed to decode field %s: %v", field.Name, err)
			}
			v.Field(i).Set(d)
		}

		if fieldsDecoded > 0 {
			if pt.Kind() == reflect.Ptr {
				return pv, nil
			}
			return v, nil
		}
	}

	// Default decoding
	d, err := decodeProto(data, t)
	if err != nil {
		return reflect.Value{}, err
	}

	if pt.Kind() == reflect.Ptr {
		ptr := reflect.New(t)
		ptr.Elem().Set(d)
		return ptr, nil
	}
	return d, nil
}

func typedDataCollectionValues(data *pb.TypedData) ([]*pb.TypedData, bool) {
	if data == nil {
		return nil, false
	}
	if collection := data.GetCollectionBytes(); collection != nil {
		values := make([]*pb.TypedData, len(collection.GetBytes()))
		for i, value := range collection.GetBytes() {
			values[i] = &pb.TypedData{Data: &pb.TypedData_Bytes{Bytes: value}}
		}
		return values, true
	}
	if collection := data.GetCollectionString(); collection != nil {
		values := make([]*pb.TypedData, len(collection.GetString_()))
		for i, value := range collection.GetString_() {
			values[i] = &pb.TypedData{Data: &pb.TypedData_String_{String_: value}}
		}
		return values, true
	}
	if collection := data.GetCollectionDouble(); collection != nil {
		values := make([]*pb.TypedData, len(collection.GetDouble()))
		for i, value := range collection.GetDouble() {
			values[i] = &pb.TypedData{Data: &pb.TypedData_Double{Double: value}}
		}
		return values, true
	}
	if collection := data.GetCollectionSint64(); collection != nil {
		values := make([]*pb.TypedData, len(collection.GetSint64()))
		for i, value := range collection.GetSint64() {
			values[i] = &pb.TypedData{Data: &pb.TypedData_Int{Int: value}}
		}
		return values, true
	}
	return nil, false
}

func collectionMetadataAt(metadata map[string]*pb.TypedData, index int) map[string]*pb.TypedData {
	result := make(map[string]*pb.TypedData, len(metadata))
	for name, value := range metadata {
		if !strings.HasSuffix(strings.ToLower(name), "array") {
			result[name] = value
		}
	}

	// Apply per-message metadata after scalar metadata so an XArray entry
	// deterministically overrides an X entry for the corresponding message.
	for name, value := range metadata {
		if !strings.HasSuffix(strings.ToLower(name), "array") {
			continue
		}
		element, ok, err := typedDataElementAt(value, index)
		if err != nil || !ok {
			continue
		}
		result[name[:len(name)-len("Array")]] = element
	}
	return result
}

func typedDataElementAt(data *pb.TypedData, index int) (*pb.TypedData, bool, error) {
	if values, ok := typedDataCollectionValues(data); ok {
		if index >= len(values) {
			return nil, false, nil
		}
		return values[index], true, nil
	}

	if data == nil || data.GetJson() == "" {
		return nil, false, nil
	}
	var values []json.RawMessage
	if err := json.Unmarshal([]byte(data.GetJson()), &values); err != nil {
		return nil, false, err
	}
	if index >= len(values) {
		return nil, false, nil
	}
	return &pb.TypedData{Data: &pb.TypedData_Json{Json: string(values[index])}}, true, nil
}

// decodeProto converts a single TypedData value from the host into a Go
// reflect.Value of the requested type t. It is called per-field when
// populating struct-based trigger messages (e.g. ServiceBusMessage,
// QueueMessage) from trigger metadata, and also for scalar argument types.
//
// TypedData is a protobuf oneof — the host extension chooses which variant
// to populate (String_, Json, Bytes, Int, Double, Http). Different
// extensions make different choices for the same logical data type:
//
//   - ServiceBus sends time fields as TypedData_String_ ("2026-06-22T...")
//   - Storage Queues sends time fields as TypedData_Json ("\"2026-06-22T...\"")
//   - DequeueCount comes as TypedData_Json (bare number: 1) not TypedData_Int
//
// The decoding strategy is:
//  1. Try the "natural" TypedData variant for the target Go type (e.g.
//     String_ for string, Int for int64, Bytes for []byte).
//  2. If the natural variant is empty/zero, fall through to the generic
//     fallback paths at the bottom of the function (JSON unmarshal,
//     string-to-T conversion, bytes-to-T).
//
// This two-phase approach ensures backward compatibility — triggers that
// already work with TypedData_String_ keep working — while also handling
// extensions that send values via a different variant.
func decodeProto(data *pb.TypedData, t reflect.Type) (reflect.Value, error) {
	if data == nil {
		return reflect.Zero(t), nil
	}

	switch t.Kind() {
	case reflect.String:
		if s := data.GetString_(); s != "" {
			return reflect.ValueOf(s), nil
		}
		// Fall through to JSON/bytes handling below when String_ is empty
	case reflect.Bool:
		if val := data.GetString_(); val != "" {
			return reflect.ValueOf(strings.EqualFold(val, "true")), nil
		}
		return reflect.ValueOf(data.GetInt() != 0), nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			if b := data.GetBytes(); len(b) > 0 {
				return reflect.ValueOf(b), nil
			}
			if s := data.GetString_(); s != "" {
				return reflect.ValueOf([]byte(s)), nil
			}
			// Fall through to JSON handling below — host may deliver
			// the payload as TypedData_Json (e.g. queue messages with
			// JSON bodies). The bytes fallback at the end of the
			// function will pick up GetJson() if populated.
		}
	case reflect.Int:
		if data.GetInt() != 0 {
			return reflect.ValueOf(int(data.GetInt())), nil
		}
	case reflect.Int64:
		if data.GetInt() != 0 {
			return reflect.ValueOf(data.GetInt()), nil
		}
	case reflect.Float64:
		if data.GetDouble() != 0 {
			return reflect.ValueOf(data.GetDouble()), nil
		}
	}

	// Handle HTTP Request
	isHttpReq := false
	if t == reflect.TypeOf(http.Request{}) || t == reflect.TypeOf((*http.Request)(nil)).Elem() {
		isHttpReq = true
	} else if t.Kind() == reflect.Ptr && t.Elem().Name() == "Request" {
		isHttpReq = true
	}

	if isHttpReq {
		if data.GetHttp() != nil {
			req, err := RpcHttpToRequest(data.GetHttp())
			if err != nil {
				return reflect.Value{}, err
			}
			return reflect.ValueOf(req).Elem(), nil
		}
	}

	// JSON fallback. Skip when target is []byte — for byte slices we want
	// to preserve the raw JSON payload (handled by the "Json handling
	// (bytes support)" block below) rather than attempting to unmarshal a
	// JSON document into a []byte (which would fail).
	if data.GetJson() != "" && !(t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8) {
		v := reflect.New(t).Interface()
		err := json.Unmarshal([]byte(data.GetJson()), v)
		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(v).Elem(), nil
	}

	// JSON-as-string fallback for slice and map types. Some host extensions
	// (notably the SQL trigger) deliver structurally-JSON payloads via
	// TypedData_String_ instead of TypedData_Json. The struct branch below
	// already handles this case; slices and maps need parallel handling.
	//
	// Unlike the struct branch, we propagate json.Unmarshal errors here:
	// a target of []SomeType or map[K]V is unambiguous about its intent,
	// so a malformed payload should surface as an explicit decode error
	// rather than silently degrade to an empty value via reflect.Zero(t).
	// []byte takes the precedence string→bytes path below, not JSON.
	if val := data.GetString_(); val != "" && (t.Kind() == reflect.Slice || t.Kind() == reflect.Map) &&
		!(t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8) {
		v := reflect.New(t).Interface()
		if err := json.Unmarshal([]byte(val), v); err != nil {
			return reflect.Value{}, fmt.Errorf("decode %s from string-typed JSON payload: %w", t, err)
		}
		return reflect.ValueOf(v).Elem(), nil
	}

	// String handling
	if val := data.GetString_(); val != "" {
		if t.Kind() == reflect.String {
			return reflect.ValueOf(val), nil
		}
		if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
			return reflect.ValueOf([]byte(val)), nil
		}
		// Try JSON unmarshal for structs when data arrives as string
		if t.Kind() == reflect.Struct {
			v := reflect.New(t).Interface()
			if err := json.Unmarshal([]byte(val), v); err == nil {
				return reflect.ValueOf(v).Elem(), nil
			}
		}
	}

	// Bytes handling
	if val := data.GetBytes(); val != nil {
		if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
			return reflect.ValueOf(val), nil
		}
		if t.Kind() == reflect.String {
			return reflect.ValueOf(string(val)), nil
		}
	}

	// Json handling (bytes support)
	if val := data.GetJson(); val != "" {
		if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
			return reflect.ValueOf([]byte(val)), nil
		}
	}

	return reflect.Zero(t), nil
}

// encodeHTTPResponse encodes a ResponseWriterProxy result into an RPC HTTP response.
func encodeHTTPResponse(proxy *ResponseWriterProxy) *pb.TypedData {
	result := proxy.Result()

	headers := make(map[string]string)
	for k, v := range result.Header {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}

	var bodyBytes []byte
	if result.Body != nil {
		bodyBytes, _ = io.ReadAll(result.Body)
		result.Body.Close()
	}

	return &pb.TypedData{
		Data: &pb.TypedData_Http{
			Http: &pb.RpcHttp{
				StatusCode: fmt.Sprintf("%d", result.StatusCode),
				Headers:    headers,
				Body: &pb.TypedData{
					Data: &pb.TypedData_Bytes{
						Bytes: bodyBytes,
					},
				},
			},
		},
	}
}

// Needed for context type checking
var contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
