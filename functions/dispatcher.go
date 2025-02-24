package functions

import (
	"log"
	"sync"

	pb "github.com/azure/azure-functions-golang-worker/proto"
)

type Dispatcher struct {
	WorkerStartupConfig *WorkerStartupConfig
	FunctionRegistry    *FunctionRegistry
}

func createDispatcher(workerStartupConfig WorkerStartupConfig) *Dispatcher {
	return &Dispatcher{
		WorkerStartupConfig: &workerStartupConfig,
		FunctionRegistry: &FunctionRegistry{
			functions: sync.Map{},
		},
	}
}

func (disp *Dispatcher) processRequestMessage(reqMsg *pb.StreamingMessage) (*pb.StreamingMessage, error) {
	switch content := reqMsg.GetContent().(type) {
	case *pb.StreamingMessage_WorkerInitRequest:
		log.Println("Handling WorkerInitRequest")
		return handleWorkerInitRequest(content.WorkerInitRequest, reqMsg.RequestId), nil
	case *pb.StreamingMessage_FunctionsMetadataRequest:
		log.Println("Handling FunctionsMetadataRequest")
		return handleFunctionsMetadataRequest(content.FunctionsMetadataRequest, reqMsg.RequestId)
	case *pb.StreamingMessage_InvocationRequest:
		log.Println("Handling InvocationRequest")
		return handleInvocationRequest(content.InvocationRequest, disp.FunctionRegistry, reqMsg.RequestId)
	case *pb.StreamingMessage_FunctionLoadRequest:
		log.Println("Handling FunctionLoadRequest")
		return handleFunctionLoadRequest(content.FunctionLoadRequest, reqMsg.RequestId), nil
	case *pb.StreamingMessage_WorkerStatusRequest:
		log.Println("Handling WorkerStatusRequest")
		return handleWorkerStatusRequest(reqMsg.RequestId, content.WorkerStatusRequest)
	case *pb.StreamingMessage_WorkerTerminate:
		log.Println("Handling WorkerTerminate")
		return handleWorkerTerminate(reqMsg.RequestId, content.WorkerTerminate)
	default:
		log.Printf("Received unhandled message type: %T\n", content)
		return nil, nil
	}
}
