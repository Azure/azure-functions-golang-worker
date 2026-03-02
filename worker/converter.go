package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"

	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// funcField represents a representation of a func field
// Used to map binding names to function arguments/return values.
type funcField struct {
	Name       string
	Type       reflect.Type
	Position   int
	Direction  string                 // "in" or "out"
	IsArgument bool                   // true if mapped to function argument, false if return value
	IsWriter   bool                   // true if this argument is an http.ResponseWriter
	Config     map[string]interface{} // The raw JSON configuration for this binding (e.g. path, connection)
}

// FromProto converts protobuf parameters to golang values
// Only processes "in" bindings mapped to Arguments.
func FromProto(req *pb.InvocationRequest, fields map[string]*funcField, args []reflect.Value) error {
	for _, input := range req.InputData {
		param, ok := fields[input.Name]
		if ok && param.Direction == "in" && param.IsArgument {
			if param.Position < len(args) {
				r, err := convertToTypeValue(param.Type, input.GetData(), req.GetTriggerMetadata(), param.Config)
				if err != nil {
					return err
				}
				args[param.Position] = r
			}
		}
	}
	return nil
}

// ToProto converts Values to grpc protocol results
// args: The arguments passed TO the function (populated with output pointers).
// results: The return values FROM the function.
// fields: The output bindings metadata.
func ToProto(args []reflect.Value, results []reflect.Value, fields map[string]*funcField) ([]*pb.ParameterBinding, *pb.TypedData, *pb.StatusResult, error) {
	// Identify max index for protoData list?
	// The protoData list order doesn't strictly matter as they are named,
	// but keeping them in order of definition (Position?) is tricky if mixed.
	// Actually, we can return a list of defined bindings.

	var protoData []*pb.ParameterBinding
	status := &pb.StatusResult{
		Status: pb.StatusResult_Success,
	}
	var returnValue *pb.TypedData
	var err error

	for _, v := range fields {
		if v.Direction != "out" {
			continue
		}

		var valToEncode reflect.Value

		if v.IsArgument {
			// It should be a pointer argument that was written to
			if v.Position < len(args) {
				arg := args[v.Position]

				if v.IsWriter {
					// It's a ResponseWriterProxy
					if proxy, ok := arg.Interface().(*ResponseWriterProxy); ok {
						valToEncode = reflect.ValueOf(proxy.Result()).Elem()
					}
				} else if arg.Kind() == reflect.Ptr && !arg.IsNil() {
					valToEncode = arg.Elem()
				}
			}
		} else {
			// It is a return value
			if v.Position < len(results) {
				valToEncode = results[v.Position]
			}
		}

		if valToEncode.IsValid() {
			d, err := encodeProto(valToEncode)
			if err != nil {
				// Log error?
				d = &pb.TypedData{}
			}

			// Special handling for $return: Put it in ReturnValue field
			if v.Name == "$return" {
				returnValue = d
			} else {
				protoData = append(protoData, &pb.ParameterBinding{
					Name: v.Name,
					RpcData: &pb.ParameterBinding_Data{
						Data: d,
					},
				})
			}
		}
	}

	// Check for error in return values
	errIndex := -1
	for k, v := range results {
		if v.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) && !v.IsNil() {
			errIndex = k
			e := v.Interface().(error)
			status.Exception = &pb.RpcException{
				Message: e.Error(),
				Source:  "User function",
			}
			status.Status = pb.StatusResult_Failure
			break
		}
	}

	// Determine the main return value ($return) if not already set
	// Use heuristic: First return value that is NOT an error and NOT mapped to an output binding?

	// Build map of used return positions
	usedReturns := make(map[int]bool)
	for _, v := range fields {
		if !v.IsArgument && v.Direction == "out" {
			usedReturns[v.Position] = true
		}
	}

	if returnValue == nil && len(results) > 0 {
		for i, v := range results {
			if i == errIndex {
				continue
			}
			if usedReturns[i] {
				continue
			}

			// Found a candidate for $return
			returnValue, err = encodeProto(v)
			if err != nil {
				return nil, nil, status, err
			}
			break
		}
	}

	return protoData, returnValue, status, nil
}

// convertToTypeValue returns a native value from protobuf
func convertToTypeValue(pt reflect.Type, data *pb.TypedData, tm map[string]*pb.TypedData, config map[string]interface{}) (reflect.Value, error) {
	// Check for Custom Converter via SDK Registry
	if fn, found := sdk.GetConverter(pt); found {
		return fn(context.Background(), config, data, tm)
	}

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
		// Try direct JSON unmarshal from input data first
		if jsonStr := data.GetJson(); jsonStr != "" {
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

			var td *pb.TypedData
			if strings.EqualFold(tag, "azfuncdata") {
				td = data
				fieldsDecoded++
			} else if val, ok := tm[tag]; ok {
				td = val
				fieldsDecoded++
			} else {
				continue
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

	// Json handling (already partly above, but ensure bytes support)
	if val := data.GetJson(); val != "" {
		if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
			return reflect.ValueOf([]byte(val)), nil
		}
	}

	return reflect.Zero(t), nil
}

func encodeProto(v reflect.Value) (*pb.TypedData, error) {
	if !v.IsValid() {
		return nil, nil
	}
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, nil
		}
		v = v.Elem()
	}

	// Handle http.Response
	if v.Type() == reflect.TypeOf(http.Response{}) {
		resp := v.Interface().(http.Response)
		rpcHttp, err := encodeHTTP(&resp)
		if err != nil {
			return nil, err
		}
		return &pb.TypedData{
			Data: &pb.TypedData_Http{
				Http: rpcHttp,
			},
		}, nil
	}

	// Default JSON
	b, err := json.Marshal(v.Interface())
	if err != nil {
		return nil, err
	}
	return &pb.TypedData{
		Data: &pb.TypedData_Json{
			Json: string(b),
		},
	}, nil
}

func encodeHTTP(resp *http.Response) (*pb.RpcHttp, error) {
	if resp == nil {
		return nil, nil
	}

	headers := make(map[string]string)
	for k, v := range resp.Header {
		if len(v) > 0 {
			headers[k] = v[0]
		}
	}

	var body *pb.TypedData
	if resp.Body != nil {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		resp.Body.Close()
		body = &pb.TypedData{
			Data: &pb.TypedData_Bytes{
				Bytes: b,
			},
		}
	}

	return &pb.RpcHttp{
		StatusCode: fmt.Sprintf("%d", resp.StatusCode),
		Headers:    headers,
		Body:       body,
	}, nil
}
