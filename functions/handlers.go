package functions

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"time"

	// Import the generated protobuf code for Azure Functions
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

func handleFunctionsMetadataRequest(req *pb.FunctionsMetadataRequest, fr *FunctionRegistry, reqID string) (*pb.StreamingMessage, error) {
	// functionAppDir := req.GetFunctionAppDirectory()
	// scriptFileName := GetAppSetting(GoScriptFileName, GoScriptFileNameDefault)
	// functionPath := filepath.Join(functionAppDir, scriptFileName)
	// log.Println("Recevied FunctionMetadataRequest with functionPath:", functionPath)
	funcId := "0f7b4505-98b8-4bd2-b71a-3ec427bd4c58"
	fi, err := fr.getFunction(funcId)
	if err != nil {
		return nil, fmt.Errorf("failed to get function info for ID %s: %v", funcId, err)
	}

	// Directory:  fi.Directory,
	// ScriptFile: "main.go",
	// EntryPoint: fi.Name,
	// Name:       fi.Name,
	// RawBindings:   fi.,

	jsonBytes, _ := json.Marshal(fi.TriggerMetadata)
	resp := &pb.StreamingMessage{
		RequestId: reqID,
		Content: &pb.StreamingMessage_FunctionMetadataResponse{
			FunctionMetadataResponse: &pb.FunctionMetadataResponse{
				FunctionMetadataResults: []*pb.RpcFunctionMetadata{
					{
						Name:       "HttpTrigger",
						Directory:  "Dir",
						ScriptFile: "main.go",
						EntryPoint: "HttpTrigger",
						Bindings: map[string]*pb.BindingInfo{
							fi.TriggerMetadata["name"]: {
								Type: fi.TriggerMetadata["type"],
							},
						},
						Language: "golang",
						RawBindings: []string{
							string(jsonBytes),
						},
						FunctionId: "0f7b4505-98b8-4bd2-b71a-3ec427bd4c58",
						Properties: map[string]string{
							"worker_indexed": "True",
						},
					},
				},
				Result: &pb.StatusResult{
					Status: pb.StatusResult_Success,
				},
			},
		},
	}

	return resp, nil
}

func handleFunctionLoadRequest(req *pb.FunctionLoadRequest, reqId string) *pb.StreamingMessage {
	return &pb.StreamingMessage{
		RequestId: reqId,
		Content: &pb.StreamingMessage_FunctionLoadResponse{
			FunctionLoadResponse: &pb.FunctionLoadResponse{
				FunctionId: req.FunctionId,
				Result: &pb.StatusResult{
					Status: pb.StatusResult_Success,
				},
			},
		},
	}
}

func handleInvocationRequest(req *pb.InvocationRequest, fr *FunctionRegistry, reqID string) (*pb.StreamingMessage, error) {
	invocationTime := time.Now().UTC()
	invocationId := req.InvocationId
	functionId := req.FunctionId
	inputData := req.InputData[0].GetData()
	docString := inputData.GetString_()
	if inputData == nil || docString == "" {
		return nil, fmt.Errorf("inputData is nil")
	}

	funcInfo, err := fr.getFunction(functionId)
	if err != nil {
		return nil, fmt.Errorf("failed to get function info for ID %s: %v", functionId, err)
	}

	funcInvocationLog := fmt.Sprintf("Function Name: %s, Invocation ID: %s, Function ID: %s, Time: %s",
		funcInfo.Name, invocationId, functionId, invocationTime)
	log.Println(funcInvocationLog)

	docs := DeserializeCosmosDocument(docString)
	fType := reflect.TypeOf(funcInfo.Func)

	inputs := make([]reflect.Value, fType.NumIn())
	for i := 0; i < fType.NumIn(); i++ {
		inputs[i] = reflect.ValueOf(docs)
	}
	reflect.ValueOf(funcInfo.Func).Call(inputs)

	resultData := &pb.TypedData{
		Data: &pb.TypedData_String_{
			String_: fmt.Sprintf("Executed (Function ID: %s)", req.FunctionId),
		},
	}

	// 3. Build an InvocationResponse containing the output data
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

// func handleInvocationRequest(req *pb.InvocationRequest, fr *FunctionRegistry, reqID string) (*pb.StreamingMessage, error) {
// 	fi, err := fr.getFunction(req.FunctionId)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return nil, fmt.Errorf("input Data: %s, trigger metadata: %s", req.GetInputData()[0], req.GetTriggerMetadata()["direction"])

// 	// resultData := &pb.TypedData{
// 	// 	Data: &pb.TypedData_String_{
// 	// 		String_: fmt.Sprintf("Executed (Function ID: %s)", req.FunctionId),
// 	// 	},
// 	// }

// 	// resp := &pb.StreamingMessage{
// 	// 	RequestId: reqID,
// 	// 	Content: &pb.StreamingMessage_InvocationResponse{
// 	// 		InvocationResponse: &pb.InvocationResponse{
// 	// 			InvocationId: req.InvocationId,
// 	// 			Result: &pb.StatusResult{
// 	// 				Status: pb.StatusResult_Success,
// 	// 			},
// 	// 			ReturnValue: resultData,
// 	// 		},
// 	// 	},
// 	// }

// 	// return resp, nil
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
