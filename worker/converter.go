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

	// If struct, try JSON decoding first, then fall back to TriggerMetadata field matching
	if t.Kind() == reflect.Struct {
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

		// Fall back to field-by-field matching from TriggerMetadata
		fieldsDecoded := 0
		for i := 0; i < v.NumField(); i++ {
			field := t.Field(i)
			tag := field.Tag.Get("json")
			// Strip omitempty or other options from the json tag
			if idx := strings.Index(tag, ","); idx != -1 {
				tag = tag[:idx]
			}

			var td *pb.TypedData
			if strings.EqualFold(tag, "azfuncdata") {
				td = data
				fieldsDecoded++
			} else if val, ok := tm[tag]; ok {
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

func decodeProto(data *pb.TypedData, t reflect.Type) (reflect.Value, error) {
	if data == nil {
		return reflect.Zero(t), nil
	}

	switch t.Kind() {
	case reflect.String:
		return reflect.ValueOf(data.GetString_()), nil
	case reflect.Bool:
		if val := data.GetString_(); val != "" {
			return reflect.ValueOf(strings.EqualFold(val, "true")), nil
		}
		return reflect.ValueOf(data.GetInt() != 0), nil
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			if len(data.GetBytes()) == 0 {
				strVal := data.GetString_()
				if strVal != "" {
					return reflect.ValueOf([]byte(strVal)), nil
				}
			}
			return reflect.ValueOf(data.GetBytes()), nil
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

	// JSON fallback
	if data.GetJson() != "" {
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
