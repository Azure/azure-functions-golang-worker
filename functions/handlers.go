package functions

import (
	"fmt"
	"log"
	"path/filepath"
	"reflect"
	"time"

	// Import the generated protobuf code for Azure Functions
	pb "github.com/azure/azure-functions-golang-worker/proto"
	"github.com/azure/azure-functions-golang-worker/sdk"
)

func handleWorkerInitRequest(req *pb.WorkerInitRequest, reqID string) *pb.StreamingMessage {
	return &pb.StreamingMessage{
		RequestId: reqID,
		Content: &pb.StreamingMessage_WorkerInitResponse{
			WorkerInitResponse: &pb.WorkerInitResponse{
				Result: &pb.StatusResult{
					Status: pb.StatusResult_Success,
				},
				WorkerVersion: GoWorkerVersion,
			},
		},
	}
}

func handleFunctionsMetadataRequest(req *pb.FunctionsMetadataRequest, reqID string) (*pb.StreamingMessage, error) {
	functionAppDir := req.GetFunctionAppDirectory()
	scriptFileName := GetAppSetting(GoScriptFileName, GoScriptFileNameDefault)
	functionPath := filepath.Join(functionAppDir, scriptFileName)

	log.Println("Recevied FunctionMetadataRequest with functionPath:", functionPath)

	resp := &pb.StreamingMessage{
		RequestId: reqID,
		Content: &pb.StreamingMessage_FunctionMetadataResponse{
			FunctionMetadataResponse: &pb.FunctionMetadataResponse{
				FunctionMetadataResults: []*pb.RpcFunctionMetadata{
					{},
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

	docs := sdk.DeserializeCosmosDocument(docString)
	fType := reflect.TypeOf(funcInfo.Func)

	inputs := make([]reflect.Value, fType.NumIn())
	for i := 0; i < fType.NumIn(); i++ {
		inputs[i] = reflect.ValueOf(docs)
	}
	reflect.ValueOf(funcInfo.Func).Call(inputs)

	resultData := &pb.TypedData{
		Data: &pb.TypedData_String_{
			String_: fmt.Sprintf("Hello from Go worker! (Function ID: %s)", req.FunctionId),
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
