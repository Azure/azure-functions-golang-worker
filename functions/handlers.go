package functions

import (
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	pb "github.com/azure/azure-functions-golang-worker/proto"
)

func handleWorkerInitRequest(req *pb.WorkerInitRequest, reqID string) *pb.StreamingMessage {
	port, err := getUnusedTCPPort()
	if err != nil {
		fmt.Println("Could not get unused port:", err)
	}

	go StartHttpServer(port)
	httpUri := fmt.Sprintf("http://127.0.0.1:%d", port)

	return &pb.StreamingMessage{
		RequestId: reqID,
		Content: &pb.StreamingMessage_WorkerInitResponse{
			WorkerInitResponse: &pb.WorkerInitResponse{
				Result: &pb.StatusResult{
					Status: pb.StatusResult_Success,
				},
				Capabilities: map[string]string{
					"HttpUri": httpUri,
				},
				WorkerVersion: GoWorkerVersion,
			},
		},
	}
}

func handleFunctionsMetadataRequest(req *pb.FunctionsMetadataRequest, registeredFuncs *sync.Map) *pb.StreamingMessage {
	var metadataResults []*pb.RpcFunctionMetadata
	registeredFuncs.Range(func(_, val interface{}) bool {
		rf := val.(RegisteredFunction)
		rpcFuncMetadata := &pb.RpcFunctionMetadata{
			Name:         rf.FuncName,
			FunctionId:   rf.FuncId,
			Language:     GoWorkerLanguage,
			ScriptFile:   GoScriptFileNameDefault, // Not used, but Host requires it
			RetryOptions: BuildRpcRetry(rf.Retry),
			RawBindings:  BuildRpcRawBindings(rf.RawBindings),
			Bindings:     GetBindingInfoList(rf.RawBindings),
		}
		metadataResults = append(metadataResults, rpcFuncMetadata)
		return true
	})

	resp := &pb.StreamingMessage{
		Content: &pb.StreamingMessage_FunctionMetadataResponse{
			FunctionMetadataResponse: &pb.FunctionMetadataResponse{
				FunctionMetadataResults: metadataResults,
				Result: &pb.StatusResult{
					Status: pb.StatusResult_Success,
				},
			},
		},
	}
	return resp
}

func handleFunctionLoadRequest(req *pb.FunctionLoadRequest, disp *Dispatcher, reqId string) *pb.StreamingMessage {
	funcId := req.GetFunctionId()
	metadata := req.GetMetadata()
	funcName := metadata.GetName()
	bindings := metadata.GetBindings()

	if _, exists := disp.LoadedFunctions.Load(funcId); exists {
		panic(fmt.Sprintf("Function with ID %s is already loaded", funcId))
	}

	inputBindings := make(map[string]GrpcBindingMetadata)
	outputBindings := make(map[string]GrpcBindingMetadata)
	for name, bind := range bindings {
		if bind.GetDirection() == pb.BindingInfo_in {
			inputBindings[name] = GrpcBindingMetadata{
				Name:      name,
				Type:      bind.GetType(),
				Direction: In,
			}
		} else {
			outputBindings[name] = GrpcBindingMetadata{
				Name:      name,
				Type:      bind.GetType(),
				Direction: Out,
			}
		}
	}

	funcDef, _ := disp.getFunction(funcId)
	funcInspect := reflect.TypeOf(funcDef)
	if funcInspect.Kind() != reflect.Func {
		panic(fmt.Sprintf("Function with ID %s is not a valid function", funcId))
	}

	var params = make([]Parameter, funcInspect.NumIn())
	for i := range funcInspect.NumIn() {
		paramType := funcInspect.In(i)
		if paramType == reflect.TypeOf(Context{}) {
		}
		// Check for functions type
		params[i] = Parameter{
			Name:     fmt.Sprintf("param%d", i),
			DataType: paramType,
		}
	}

	disp.LoadedFunctions.Store(funcId, FunctionDefinition{
		FuncId:         funcId,
		FuncName:       funcName,
		InputBindings:  inputBindings,
		OutputBindings: outputBindings,
	})

	fmt.Printf("Input Bindings: %+v\n", inputBindings)
	fmt.Printf("Output Bindings: %+v\n", outputBindings)

	return &pb.StreamingMessage{
		RequestId: reqId,
		Content: &pb.StreamingMessage_FunctionLoadResponse{
			FunctionLoadResponse: &pb.FunctionLoadResponse{
				FunctionId: funcId,
				Result: &pb.StatusResult{
					Status: pb.StatusResult_Success,
				},
			},
		},
	}
}

func handleInvocationRequest(req *pb.InvocationRequest, disp *Dispatcher, reqID string) (*pb.StreamingMessage, error) {
	invocTime := time.Now().UTC()
	invocId := req.GetInvocationId()
	funcId := req.GetFunctionId()
	// rf, err := disp.getFunction(funcId)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to get function with ID %s: %v", funcId, err)
	// }

	funcVal, found := disp.LoadedFunctions.Load(funcId)
	if !found {
		return nil, fmt.Errorf("function with ID %s not found", funcId)
	}

	res, casted := funcVal.(*FunctionDefinition)
	if !casted {
		return nil, fmt.Errorf("failed to cast RegisteredFunction for ID %s", funcId)
	}

	funcInvocationLog := fmt.Sprintf("Invocation request received. Function Name: %s, Invocation ID: %s, Function ID: %s, Time: %s",
		res.FuncName, invocId, funcId, invocTime)
	log.Println(funcInvocationLog)

	// Consider caching input here

	// Get bindings

	// docs := DeserializeCosmosDocument("{}") // Replace with actual deserialization logic
	// fType := reflect.TypeOf(rf.Func)

	// inputs := make([]reflect.Value, fType.NumIn())
	// for i := 0; i < fType.NumIn(); i++ {
	// 	inputs[i] = reflect.ValueOf(docs)
	// }
	// reflect.ValueOf(rf.Func).Call(inputs)

	resultData := &pb.TypedData{
		Data: &pb.TypedData_String_{
			String_: fmt.Sprintf("Executed (Function ID: %s)", req.FunctionId),
		},
	}

	resp := &pb.StreamingMessage{
		RequestId: reqID,
		Content: &pb.StreamingMessage_InvocationResponse{
			InvocationResponse: &pb.InvocationResponse{
				InvocationId: req.InvocationId,
				Result: &pb.StatusResult{
					Status: pb.StatusResult_Success,
				},
				ReturnValue: resultData,
			},
		},
	}

	return resp, nil
}

// func handleInvocationRequest(req *pb.InvocationRequest, fr *FunctionRegistry, reqID string) (*pb.StreamingMessage, error) {
// 	invocationTime := time.Now().UTC()
// 	invocId := req.InvocationId
// 	functionId := req.FunctionId

// 	httpReq, respWriter, err := globalCoordinator.GetHTTPRequest(invocId)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to get HTTP request: %w", err)
// 	}

// 	funcInfo, err := fr.getFunction(functionId)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to get function info for ID %s: %v", functionId, err)
// 	}

// 	// routeParam := funcInfo.TriggerMetadata["param_name"]
// 	// SyncRouteParams(httpReq, routeParams)

// 	funcInvocationLog := fmt.Sprintf("Function Name: %s, Invocation ID: %s, Function ID: %s, Time: %s",
// 		funcInfo.Name, invocId, functionId, invocationTime)
// 	log.Println(funcInvocationLog)

// 	fType := reflect.TypeOf(funcInfo.Func)
// 	inputs := make([]reflect.Value, fType.NumIn())
// 	inputs[0] = reflect.ValueOf(respWriter)
// 	inputs[1] = reflect.ValueOf(httpReq)
// 	log.Println("Calling function...")
// 	go reflect.ValueOf(funcInfo.Func).Call(inputs)
// 	log.Println("Called function")

// 	globalCoordinator.NotifyResponseReady(invocId)
// 	// for i, param := range req.InputData {
// 	// 	fmt.Printf("Binding %d: Name=%s\n", i, param.Name)

// 	// 	switch data := param.RpcData.(type) {
// 	// 	case *pb.ParameterBinding_Data:
// 	// 		fmt.Printf("  Data: %+v\n", data.Data)
// 	// 	case *pb.ParameterBinding_RpcSharedMemory:
// 	// 		fmt.Printf("  SharedMemory: %+v\n", data.RpcSharedMemory)
// 	// 	default:
// 	// 		fmt.Println("  Unknown RpcData type")
// 	// 	}
// 	// }
// 	// return nil, fmt.Errorf("inputData is: %s, GetString() is: %s", inputData, mystr)

// 	resultData := &pb.TypedData{
// 		Data: &pb.TypedData_String_{
// 			String_: fmt.Sprintf("Executed (Function ID: %s)", req.FunctionId),
// 		},
// 	}

// 	resp := &pb.StreamingMessage{
// 		RequestId: reqID,
// 		Content: &pb.StreamingMessage_InvocationResponse{
// 			InvocationResponse: &pb.InvocationResponse{
// 				InvocationId: req.InvocationId,
// 				Result: &pb.StatusResult{
// 					Status: pb.StatusResult_Success,
// 				},
// 				ReturnValue: resultData,
// 			},
// 		},
// 	}

// 	return resp, nil
// }

// handleWorkerStatusRequest handles periodic status checks from the host.
func handleWorkerStatusRequest(
	requestID string,
	req *pb.WorkerStatusRequest,
) (*pb.StreamingMessage, error) {

	// Typically, you’d check health metrics or resource usage here.
	// For now, just return a success status.

	resp := &pb.StreamingMessage{
		RequestId: requestID,
		Content: &pb.StreamingMessage_WorkerStatusResponse{
			WorkerStatusResponse: &pb.WorkerStatusResponse{
				// No fields in the WorkerStatusResponse, but we can show success if needed
			},
		},
	}

	return resp, nil
}

// handleWorkerTerminate responds to a WorkerTerminate request from the host.
func handleWorkerTerminate(
	requestID string,
	req *pb.WorkerTerminate,
) (*pb.StreamingMessage, error) {

	// The host wants us to shut down. We might set a flag or begin graceful shutdown logic.
	// Typically, the host does not require a response, but we can log or provide minimal feedback.

	// For now, we’ll return nil, meaning we have no explicit response.
	// Or we could return a StreamingMessage if we want to confirm termination.

	return nil, nil
}
