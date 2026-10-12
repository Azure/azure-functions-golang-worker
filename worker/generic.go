package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/azure/azure-functions-golang-worker/internal/bindingtype"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// decodeGenericInput is the explicit raw-or-JSON contract for generic bindings.
// Legacy trigger models retain their metadata-field matching in convertToTypeValue.
// Both are used by the same argument binder and invocation pipeline.
func decodeGenericInput(t reflect.Type, data *pb.TypedData) (value reflect.Value, err error) {
	// JSON unmarshaling can execute application code before the handler's panic
	// boundary. Keep a custom unmarshaler panic local to this invocation.
	defer func() {
		if recovered := recover(); recovered != nil {
			value = reflect.Value{}
			err = fmt.Errorf("decode %s: %v", t, recovered)
		}
	}()
	if data == nil || data.Data == nil {
		return reflect.Value{}, fmt.Errorf("missing payload")
	}
	if t.Kind() == reflect.Ptr {
		base, err := bindingtype.Base(t)
		if err != nil {
			return reflect.Value{}, err
		}
		if !bindingtype.IsRaw(base) && genericInputIsNull(data) {
			return reflect.Zero(t), nil
		}
		value, err := decodeGenericInput(t.Elem(), data)
		if err != nil {
			return reflect.Value{}, err
		}
		ptr := reflect.New(t.Elem())
		ptr.Elem().Set(value)
		return ptr.Convert(t), nil
	}
	if values, ok := typedDataCollectionValues(data); ok {
		if t.Kind() != reflect.Slice || bindingtype.IsBytes(t) {
			return reflect.Value{}, fmt.Errorf("host collection requires a slice, got %s", t)
		}
		if reflect.PointerTo(t).Implements(reflect.TypeFor[json.Unmarshaler]()) {
			array, err := genericCollectionJSON(t.Elem(), values)
			if err != nil {
				return reflect.Value{}, err
			}
			return decodeGenericJSON(t, bytes.NewReader(array))
		}
		result := reflect.MakeSlice(t, len(values), len(values))
		for i, item := range values {
			value, err := decodeGenericInput(t.Elem(), item)
			if err != nil {
				return reflect.Value{}, fmt.Errorf("element %d: %w", i, err)
			}
			result.Index(i).Set(value)
		}
		return result, nil
	}
	if bindingtype.IsRaw(t) {
		return decodeGenericRaw(t, data)
	}
	if genericInputIsNull(data) && t.Kind() != reflect.Map && t.Kind() != reflect.Slice && t.Kind() != reflect.Interface {
		return reflect.Value{}, fmt.Errorf("null is not valid for %s; use a pointer for nullable payloads", t)
	}
	reader, err := genericJSONReader(data)
	if err != nil {
		return reflect.Value{}, err
	}
	return decodeGenericJSON(t, reader)
}

func genericInputIsNull(data *pb.TypedData) bool {
	switch value := data.Data.(type) {
	case *pb.TypedData_String_:
		return strings.TrimSpace(value.String_) == "null"
	case *pb.TypedData_Json:
		return strings.TrimSpace(value.Json) == "null"
	case *pb.TypedData_Bytes:
		return bytes.Equal(bytes.TrimSpace(value.Bytes), []byte("null"))
	}
	return false
}

// Keep text as text and bytes as bytes. Matching raw representations do not
// allocate payload-sized buffers; cross-representation conversions are explicit.
func decodeGenericRaw(t reflect.Type, data *pb.TypedData) (reflect.Value, error) {
	var text string
	var raw []byte
	binary := false
	switch value := data.Data.(type) {
	case *pb.TypedData_String_:
		text = value.String_
	case *pb.TypedData_Json:
		text = value.Json
	case *pb.TypedData_Bytes:
		raw, binary = value.Bytes, true
	case *pb.TypedData_Int:
		text = fmt.Sprint(value.Int)
	case *pb.TypedData_Double:
		encoded, err := json.Marshal(value.Double)
		if err != nil {
			return reflect.Value{}, err
		}
		raw, binary = encoded, true
	default:
		return reflect.Value{}, fmt.Errorf("unsupported host payload kind %T", data.Data)
	}
	if bindingtype.IsText(t) {
		if binary {
			text = string(raw)
		}
		return reflect.ValueOf(text).Convert(t), nil
	}
	if !binary {
		raw = []byte(text)
	}
	value := reflect.ValueOf(raw)
	if value.CanConvert(t) {
		return value.Convert(t), nil
	}
	result := reflect.MakeSlice(t, len(raw), len(raw))
	for i, b := range raw {
		result.Index(i).SetUint(uint64(b))
	}
	return result, nil
}

func genericJSONReader(data *pb.TypedData) (io.Reader, error) {
	switch value := data.Data.(type) {
	case *pb.TypedData_String_:
		return strings.NewReader(value.String_), nil
	case *pb.TypedData_Json:
		return strings.NewReader(value.Json), nil
	case *pb.TypedData_Bytes:
		return bytes.NewReader(value.Bytes), nil
	case *pb.TypedData_Int:
		return strings.NewReader(fmt.Sprint(value.Int)), nil
	case *pb.TypedData_Double:
		encoded, err := json.Marshal(value.Double)
		return bytes.NewReader(encoded), err
	default:
		return nil, fmt.Errorf("unsupported host payload kind %T", data.Data)
	}
}

// Decode exactly one JSON value, preserving numbers in dynamic fields. A custom
// UnmarshalJSON implementation owns number handling inside its own decoder.
func decodeGenericJSON(t reflect.Type, reader io.Reader) (reflect.Value, error) {
	target := reflect.New(t)
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	if err := decoder.Decode(target.Interface()); err != nil {
		return reflect.Value{}, fmt.Errorf("decode JSON into %s: %w", t, err)
	}
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("multiple JSON values")
		}
		return reflect.Value{}, fmt.Errorf("decode JSON into %s: trailing content: %w", t, err)
	}
	return target.Elem(), nil
}

// A custom named-slice decoder receives one JSON array for either wire form.
// Preserve raw element semantics (strings and base64 byte strings); structured
// elements are inserted as JSON without invoking their custom decoder twice.
func genericCollectionJSON(element reflect.Type, values []*pb.TypedData) ([]byte, error) {
	base, err := bindingtype.Base(element)
	if err != nil {
		return nil, err
	}
	array := make([]json.RawMessage, len(values))
	for i, value := range values {
		var encoded []byte
		if bindingtype.IsRaw(base) {
			decoded, err := decodeGenericRaw(base, value)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			// Use built-in raw types so a named element's MarshalJSON cannot
			// change input normalization or run application serialization code.
			if bindingtype.IsText(base) {
				encoded, err = json.Marshal(decoded.String())
			} else {
				encoded, err = json.Marshal(decoded.Bytes())
			}
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
		} else {
			reader, err := genericJSONReader(value)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			encoded, err = io.ReadAll(reader)
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
		}
		array[i] = encoded
	}
	return json.Marshal(array)
}

func genericTriggerMetadata(in map[string]*pb.TypedData) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, data := range in {
		if data == nil {
			continue
		}
		switch value := data.Data.(type) {
		case *pb.TypedData_String_:
			out[key] = value.String_
		case *pb.TypedData_Bytes:
			out[key] = value.Bytes
		case *pb.TypedData_Json:
			out[key] = json.RawMessage(value.Json)
		case *pb.TypedData_Int:
			out[key] = value.Int
		case *pb.TypedData_Double:
			out[key] = value.Double
		case *pb.TypedData_CollectionString:
			out[key] = value.CollectionString.GetString_()
		case *pb.TypedData_CollectionBytes:
			out[key] = value.CollectionBytes.GetBytes()
		case *pb.TypedData_CollectionSint64:
			out[key] = value.CollectionSint64.GetSint64()
		case *pb.TypedData_CollectionDouble:
			out[key] = value.CollectionDouble.GetDouble()
		}
	}
	return out
}

func bindingFailureResponse(requestID, invocationID string, err error) *pb.StreamingMessage {
	return &pb.StreamingMessage{RequestId: requestID, Content: &pb.StreamingMessage_InvocationResponse{
		InvocationResponse: &pb.InvocationResponse{InvocationId: invocationID, Result: &pb.StatusResult{
			Status: pb.StatusResult_Failure, Exception: &pb.RpcException{Source: "Binding conversion", Message: err.Error()},
		}},
	}}
}
