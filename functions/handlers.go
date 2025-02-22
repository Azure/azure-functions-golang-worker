package functions

import (
	"fmt"
	"log"
	"path/filepath"

	// Import the generated protobuf code for Azure Functions
	functionrpc "github.com/azure/azure-functions-golang-worker/proto"
)

func HandleWorkerInitRequest(req *functionrpc.WorkerInitRequest, requestID string) *functionrpc.StreamingMessage {
	return &functionrpc.StreamingMessage{
		RequestId: requestID,
		Content: &functionrpc.StreamingMessage_WorkerInitResponse{
			WorkerInitResponse: &functionrpc.WorkerInitResponse{
				Result: &functionrpc.StatusResult{
					Status: functionrpc.StatusResult_Success,
				},
				WorkerVersion: GoWorkerVersion,
			},
		},
	}
}

func handleFunctionMetadataRequest(
	requestID string,
	req *functionrpc.FunctionsMetadataRequest,
) (*functionrpc.StreamingMessage, error) {
	functionAppDir := req.GetFunctionAppDirectory()
	scriptFileName := GetAppSetting(GoScriptFileName, GoScriptFileNameDefault)
	functionPath := filepath.Join(functionAppDir, scriptFileName)

	// TODO: add request ID from init
	log.Println("Recevied FunctionMetadataRequest with functionPath:", functionPath)

	// TODO: validate script file name here

	return nil, nil
}

// handleFunctionLoadRequest processes a FunctionLoadRequest from the host.
func handleFunctionLoadRequest(
	requestID string,
	req *functionrpc.FunctionLoadRequest,
	registry *FunctionRegistry, // optional: if you’re storing loaded functions
) (*functionrpc.StreamingMessage, error) {

	// // You might store the function info in the registry (stubbed here).
	// if registry != nil {
	// 	err := registry.RegisterFunction(req.FunctionId, req.Metadata)
	// 	if err != nil {
	// 		return nil, fmt.Errorf("failed to register function: %w", err)
	// 	}
	// }

	// Prepare a load response indicating success.
	resp := &functionrpc.StreamingMessage{
		RequestId: requestID,
		Content: &functionrpc.StreamingMessage_FunctionLoadResponse{
			FunctionLoadResponse: &functionrpc.FunctionLoadResponse{
				FunctionId: req.FunctionId,
				Result: &functionrpc.StatusResult{
					Status: functionrpc.StatusResult_Success,
				},
			},
		},
	}

	return resp, nil
}

// handleInvocationRequest processes an InvocationRequest (i.e., a function call) from the host.
func handleInvocationRequest(
	requestID string,
	req *functionrpc.InvocationRequest,
	registry *FunctionRegistry,
) (*functionrpc.StreamingMessage, error) {

	// Example: You might look up the function in your registry.
	// For a stub, we’ll just pretend we call the function and return a static result.

	// 1. (Optional) Find function info: registry.GetFunction(req.FunctionId)

	// 2. “Execute” it, gather outputs (here, we’re just returning a static string).
	resultData := &functionrpc.TypedData{
		Data: &functionrpc.TypedData_String_{
			String_: fmt.Sprintf("Hello from Go worker! (Function ID: %s)", req.FunctionId),
		},
	}

	// 3. Build an InvocationResponse containing the output data
	resp := &functionrpc.StreamingMessage{
		RequestId: requestID,
		Content: &functionrpc.StreamingMessage_InvocationResponse{
			InvocationResponse: &functionrpc.InvocationResponse{
				InvocationId: req.InvocationId,
				Result: &functionrpc.StatusResult{
					Status: functionrpc.StatusResult_Success,
				},
				// If this function has output bindings, you’d fill them in here.
				ReturnValue: resultData,
			},
		},
	}

	return resp, nil
}

// handleWorkerStatusRequest handles periodic status checks from the host.
func handleWorkerStatusRequest(
	requestID string,
	req *functionrpc.WorkerStatusRequest,
) (*functionrpc.StreamingMessage, error) {

	// Typically, you’d check health metrics or resource usage here.
	// For now, just return a success status.

	resp := &functionrpc.StreamingMessage{
		RequestId: requestID,
		Content: &functionrpc.StreamingMessage_WorkerStatusResponse{
			WorkerStatusResponse: &functionrpc.WorkerStatusResponse{
				// No fields in the WorkerStatusResponse, but we can show success if needed
			},
		},
	}

	return resp, nil
}

// handleWorkerTerminate responds to a WorkerTerminate request from the host.
func handleWorkerTerminate(
	requestID string,
	req *functionrpc.WorkerTerminate,
) (*functionrpc.StreamingMessage, error) {

	// The host wants us to shut down. We might set a flag or begin graceful shutdown logic.
	// Typically, the host does not require a response, but we can log or provide minimal feedback.

	// For now, we’ll return nil, meaning we have no explicit response.
	// Or we could return a StreamingMessage if we want to confirm termination.

	return nil, nil
}
