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

func handleFunctionsMetadataRequest(req *pb.FunctionsMetadataRequest, app *sdk.App) *pb.StreamingMessage {
	log.Printf("Received FunctionsMetadataRequest")
	var functions []*pb.RpcFunctionMetadata
	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		rf := value.(*sdk.RegisteredFunction)

		// Map bindings.Binding to pb.BindingInfo
		bindings := make(map[string]*pb.BindingInfo)
		var rawBindings []string

		// Analyze function arguments for DataType inference
		ft := reflect.TypeOf(rf.Func)
		argIndex := 0

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

			dataType := pb.BindingInfo_undefined

			if b.Direction != "out" {
				// Find corresponding argument
				for argIndex < ft.NumIn() {
					t := ft.In(argIndex)
					// Skip Context and ResponseWriter as they are not input data bindings
					if t.Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) ||
						t == reflect.TypeOf((*http.ResponseWriter)(nil)).Elem() {
						argIndex++
						continue
					}
					break
				}

				if argIndex < ft.NumIn() {
					t := ft.In(argIndex)
					if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
						dataType = pb.BindingInfo_binary
					}
					// Consume this argument
					argIndex++
				}
			}

			bindings[b.Name] = &pb.BindingInfo{
				Type:      b.Type,
				Direction: dir,
				DataType:  dataType,
			}

			// Serialize binding to JSON for RawBindings
			if bData, err := json.Marshal(b); err == nil {
				rawBindings = append(rawBindings, string(bData))
			}
		}

		functions = append(functions, &pb.RpcFunctionMetadata{
			FunctionId:  rf.FuncId,
			Name:        rf.FuncName,
			Bindings:    bindings,
			RawBindings: rawBindings,
			Language:    "go",
			ScriptFile:  rf.ScriptFile,
			EntryPoint:  rf.FuncName,
		})
		return true
	})

	return &pb.StreamingMessage{
		RequestId: "",
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
	val, ok := disp.App.RegisteredFunctions.Load(funcID)
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

	// Create map of bindings
	fields := make(map[string]*funcField)

	// Analyze function arguments
	ft := reflect.TypeOf(rf.Func)

	// Identify writer index (heuristic)
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

	argIndex := 0
	retIndex := 0

	for _, b := range rf.RawBindings {
		// $return is handled separately if writer exists, otherwise it's a return value
		if b.Name == "$return" {
			if writerIndex != -1 {
				// Already mapped to writer
			} else {
				fields[b.Name] = &funcField{
					Name:       b.Name,
					Direction:  b.Direction,
					IsArgument: false,
					Position:   retIndex, // Assuming first return is $return?
				}
				retIndex++
			}
			continue
		}

		// Try to match with an argument
		if argIndex < ft.NumIn() {
			// Check if this binding CAN be this argument
			argType := ft.In(argIndex)

			// Context is skipped in binding loop usually
			if argType.Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
				argIndex++
				if argIndex >= ft.NumIn() {
					break
				}
				argType = ft.In(argIndex)
			}

			// Check for http.ResponseWriter
			if argType == reflect.TypeOf((*http.ResponseWriter)(nil)).Elem() {
				// We found a ResponseWriter.
				// This implies we have an HTTP Output binding (usually named $return).
				// We map $return to this argument position for "out" usage.
				// But wait, the $return binding is already processed or will be processed.
				// If we encounter ResponseWriter, we should link it to the $return binding if possible.
				// OR we simply mark this argument index as "IsWriter" and use it later.
				// But we are iterating bindings here.

				// If we are seeing "req" input binding, but the current argument is ResponseWriter,
				// we should SKIP ResponseWriter for "req" and move to next argument.

				// SO: Iterate arguments until we find one that matches the binding OR is not special.

				argIndex++
				if argIndex < ft.NumIn() {
					argType = ft.In(argIndex)
				} else {
					// No more args to map this binding to
					break
				}
			}

			// Assign this binding to this argument
			fields[b.Name] = &funcField{
				Name:       b.Name,
				Type:       argType,
				Position:   argIndex,
				Direction:  b.Direction,
				IsArgument: true,
			}
			argIndex++
		} else {
			// If we ran out of arguments, maybe it's a return value?
			// Only for "out" bindings
			if b.Direction == "out" {
				fields[b.Name] = &funcField{
					Name:       b.Name,
					Direction:  b.Direction,
					IsArgument: false, // It's a return value
					Position:   retIndex,
				}
				retIndex++
			}
		}
	}

	// Logic to log the computed fields for debugging
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

	// 1. Pre-allocate pointer arguments
	// 2. Handle Context
	for i := 0; i < ft.NumIn(); i++ {
		t := ft.In(i)
		if t.Implements(reflect.TypeOf((*context.Context)(nil)).Elem()) {
			args[i] = reflect.ValueOf(context.Background())
			continue
		}

		if t == reflect.TypeOf((*http.ResponseWriter)(nil)).Elem() {
			args[i] = reflect.ValueOf(NewResponseWriterProxy())
			continue
		}

		if t.Kind() == reflect.Ptr {
			// Allocate new value for the pointer
			args[i] = reflect.New(t.Elem())
		} else {
			// Zero value for non-pointers (will be overwritten by FromProto if "in" binding)
			args[i] = reflect.Zero(t)
		}
	}

	// 3. Populate Inputs
	if err := FromProto(req, loadedFunc.Fields, args); err != nil {
		return nil, err
	}

	// 4. Invoke
	fv := reflect.ValueOf(loadedFunc.Function.Func)
	results := fv.Call(args)

	// 5. Extract Outputs
	outputData, returnValue, status, err := ToProto(args, results, loadedFunc.Fields)
	if err != nil {
		status = &pb.StatusResult{
			Status: pb.StatusResult_Failure,
			Exception: &pb.RpcException{
				Message: err.Error(),
			},
		}
	}

	return &pb.StreamingMessage{
		RequestId: requestId,
		Content: &pb.StreamingMessage_InvocationResponse{
			InvocationResponse: &pb.InvocationResponse{
				InvocationId: req.InvocationId,
				OutputData:   outputData,
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
	return nil, nil // Graceful shutdown managed by host
}

func handleFunctionEnvironmentReloadRequest(requestId string, req *pb.FunctionEnvironmentReloadRequest) (*pb.StreamingMessage, error) {
	// Reload env vars or other settings
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
