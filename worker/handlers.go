package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"reflect"

	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

type LoadedFunction struct {
	Function sdk.RegisteredFunction
	Fields   map[string]*funcField
}

func handleWorkerInitRequest(req *pb.WorkerInitRequest, requestId string) *pb.StreamingMessage {
	log.Printf("Received WorkerInitRequest: RequestId=%s", requestId)
	return &pb.StreamingMessage{
		RequestId: requestId,
		Content: &pb.StreamingMessage_WorkerInitResponse{
			WorkerInitResponse: &pb.WorkerInitResponse{
				Result: &pb.StatusResult{
					Status: pb.StatusResult_Success,
					Result: "Success",
				},
				WorkerVersion: "1.0.0",
			},
		},
	}
}

func handleFunctionsMetadataRequest(req *pb.FunctionsMetadataRequest, app *sdk.App, requestId string) *pb.StreamingMessage {
	log.Printf("Received FunctionsMetadataRequest: RequestId=%s", requestId)
	var functions []*pb.RpcFunctionMetadata
	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		rf := value.(*sdk.RegisteredFunction)

		// Map bindings.Binding to pb.BindingInfo
		bindingsMap := make(map[string]*pb.BindingInfo)
		var rawBindings []string

		for _, b := range rf.RawBindings {
			var dir pb.BindingInfo_Direction
			switch b.Direction {
			case "in":
				dir = pb.BindingInfo_in
			case "out":
				dir = pb.BindingInfo_out
			case "inout":
				dir = pb.BindingInfo_inout
			}

			bindingsMap[b.Name] = &pb.BindingInfo{
				Type:      b.Type,
				Direction: dir,
				DataType:  pb.BindingInfo_undefined,
			}

			if bData, err := json.Marshal(b); err == nil {
				rawBindings = append(rawBindings, string(bData))
			}
		}

		functions = append(functions, &pb.RpcFunctionMetadata{
			FunctionId:  rf.FuncId,
			Name:        rf.FuncName,
			Bindings:    bindingsMap,
			RawBindings: rawBindings,
			Language:    "go",
			ScriptFile:  rf.ScriptFile,
			EntryPoint:  rf.FuncName,
		})
		return true
	})

	return &pb.StreamingMessage{
		RequestId: requestId,
		Content: &pb.StreamingMessage_FunctionMetadataResponse{
			FunctionMetadataResponse: &pb.FunctionMetadataResponse{
				FunctionMetadataResults: functions,
				Result: &pb.StatusResult{
					Status: pb.StatusResult_Success,
					Result: "Success",
				},
			},
		},
	}
}

func handleFunctionLoadRequest(req *pb.FunctionLoadRequest, disp *Dispatcher, requestId string) *pb.StreamingMessage {
	log.Printf("Received FunctionLoadRequest: RequestId=%s, FunctionId=%s", requestId, req.FunctionId)
	funcID := req.FunctionId
	val, ok := disp.App.GetRegisteredFunctions().Load(funcID)
	if !ok {
		return &pb.StreamingMessage{
			RequestId: requestId,
			Content: &pb.StreamingMessage_FunctionLoadResponse{
				FunctionLoadResponse: &pb.FunctionLoadResponse{
					FunctionId: funcID,
					Result: &pb.StatusResult{
						Status: pb.StatusResult_Failure,
						Exception: &pb.RpcException{
							Message: fmt.Sprintf("Function with ID %s not found", funcID),
						},
					},
				},
			},
		}
	}

	rf := val.(*sdk.RegisteredFunction)
	ft := reflect.TypeOf(rf.Func)
	fields := make(map[string]*funcField)

	// Identify writer index for HTTP triggers
	writerIndex := -1
	for i := 0; i < ft.NumIn(); i++ {
		if ft.In(i) == reflect.TypeOf((*http.ResponseWriter)(nil)).Elem() {
			writerIndex = i
			break
		}
	}

	// If writer exists, map $return binding to it
	if writerIndex != -1 {
		fields["$return"] = &funcField{
			Name:       "$return",
			Type:       ft.In(writerIndex),
			Position:   writerIndex,
			Direction:  "out",
			IsArgument: true,
			IsWriter:   true,
		}
	}

	// Map trigger input binding to the appropriate argument
	argIndex := 0
	for _, b := range rf.RawBindings {
		if b.Direction != "in" {
			continue
		}

		// Find the next argument that is not context.Context or http.ResponseWriter
		for argIndex < ft.NumIn() {
			argType := ft.In(argIndex)
			if argType.Implements(contextType) || argType == reflect.TypeOf((*http.ResponseWriter)(nil)).Elem() {
				argIndex++
				continue
			}
			break
		}

		if argIndex >= ft.NumIn() {
			break
		}

		fields[b.Name] = &funcField{
			Name:       b.Name,
			Type:       ft.In(argIndex),
			Position:   argIndex,
			Direction:  b.Direction,
			IsArgument: true,
		}
		argIndex++
	}

	for k, v := range fields {
		log.Printf("Debug: Field Mapping - Name: %s, Pos: %d, Type: %v, Dir: %s, Arg: %v", k, v.Position, v.Type, v.Direction, v.IsArgument)
	}

	disp.LoadedFunctions.Store(funcID, &LoadedFunction{
		Function: *rf,
		Fields:   fields,
	})

	return &pb.StreamingMessage{
		RequestId: requestId,
		Content: &pb.StreamingMessage_FunctionLoadResponse{
			FunctionLoadResponse: &pb.FunctionLoadResponse{
				FunctionId: funcID,
				Result: &pb.StatusResult{
					Status: pb.StatusResult_Success,
					Result: "Success",
				},
			},
		},
	}
}

func handleInvocationRequest(req *pb.InvocationRequest, disp *Dispatcher, requestId string) (*pb.StreamingMessage, error) {
	log.Printf("Received InvocationRequest: RequestId=%s, InvocationId=%s, FunctionId=%s", requestId, req.InvocationId, req.FunctionId)
	funcID := req.FunctionId
	val, ok := disp.LoadedFunctions.Load(funcID)
	if !ok {
		return nil, fmt.Errorf("function with ID %s not loaded", funcID)
	}
	loadedFunc := val.(*LoadedFunction)

	ft := reflect.TypeOf(loadedFunc.Function.Func)
	args := make([]reflect.Value, ft.NumIn())

	// 1. Pre-allocate arguments
	for i := 0; i < ft.NumIn(); i++ {
		t := ft.In(i)
		if t.Implements(contextType) {
			args[i] = reflect.ValueOf(context.Background())
			continue
		}

		if t == reflect.TypeOf((*http.ResponseWriter)(nil)).Elem() {
			args[i] = reflect.ValueOf(NewResponseWriterProxy())
			continue
		}

		if t.Kind() == reflect.Ptr {
			args[i] = reflect.New(t.Elem())
		} else {
			args[i] = reflect.Zero(t)
		}
	}

	// 2. Populate trigger input
	if err := FromProto(req, loadedFunc.Fields, args); err != nil {
		return nil, err
	}

	// 3. Invoke handler
	fv := reflect.ValueOf(loadedFunc.Function.Func)
	results := fv.Call(args)

	// 4. Build response
	status := &pb.StatusResult{
		Status: pb.StatusResult_Success,
	}

	// Check for error return
	for _, r := range results {
		if r.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) && !r.IsNil() {
			e := r.Interface().(error)
			status = &pb.StatusResult{
				Status: pb.StatusResult_Failure,
				Exception: &pb.RpcException{
					Message: e.Error(),
					Source:  "User function",
				},
			}
			break
		}
	}

	// 5. Extract HTTP response if applicable
	var returnValue *pb.TypedData
	for _, field := range loadedFunc.Fields {
		if field.Direction == "out" && field.IsWriter && field.Name == "$return" {
			if field.Position < len(args) {
				if proxy, ok := args[field.Position].Interface().(*ResponseWriterProxy); ok {
					returnValue = encodeHTTPResponse(proxy)
				}
			}
		}
	}

	return &pb.StreamingMessage{
		RequestId: requestId,
		Content: &pb.StreamingMessage_InvocationResponse{
			InvocationResponse: &pb.InvocationResponse{
				InvocationId: req.InvocationId,
				ReturnValue:  returnValue,
				Result:       status,
			},
		},
	}, nil
}

func handleWorkerStatusRequest(requestId string, req *pb.WorkerStatusRequest) (*pb.StreamingMessage, error) {
	return &pb.StreamingMessage{
		RequestId: requestId,
		Content: &pb.StreamingMessage_WorkerStatusResponse{
			WorkerStatusResponse: &pb.WorkerStatusResponse{},
		},
	}, nil
}

func handleWorkerTerminate(requestId string, req *pb.WorkerTerminate) (*pb.StreamingMessage, error) {
	return nil, nil
}

func handleFunctionEnvironmentReloadRequest(requestId string, req *pb.FunctionEnvironmentReloadRequest) (*pb.StreamingMessage, error) {
	return &pb.StreamingMessage{
		RequestId: requestId,
		Content: &pb.StreamingMessage_FunctionEnvironmentReloadResponse{
			FunctionEnvironmentReloadResponse: &pb.FunctionEnvironmentReloadResponse{
				Result: &pb.StatusResult{
					Status: pb.StatusResult_Success,
					Result: "Success",
				},
			},
		},
	}, nil
}
