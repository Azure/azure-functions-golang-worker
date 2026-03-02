package worker

import (
	"log"
	"sync"

	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

type Dispatcher struct {
	WorkerStartupConfig *WorkerStartupConfig
	App                 *sdk.App
	LoadedFunctions     *sync.Map
}

func NewDispatcher(workerStartupConfig *WorkerStartupConfig, app *sdk.App) *Dispatcher {
	return &Dispatcher{
		WorkerStartupConfig: workerStartupConfig,
		App:                 app,
		LoadedFunctions:     &sync.Map{},
	}
}

func (disp *Dispatcher) processRequestMessage(reqMsg *pb.StreamingMessage) (*pb.StreamingMessage, error) {
	switch content := reqMsg.GetContent().(type) {
	case *pb.StreamingMessage_WorkerInitRequest:
		log.Println("Handling WorkerInitRequest")
		return handleWorkerInitRequest(content.WorkerInitRequest, reqMsg.RequestId), nil
	case *pb.StreamingMessage_FunctionsMetadataRequest:
		log.Println("Handling FunctionsMetadataRequest")
		return handleFunctionsMetadataRequest(content.FunctionsMetadataRequest, disp.App), nil
	case *pb.StreamingMessage_InvocationRequest:
		log.Println("Handling InvocationRequest")
		return handleInvocationRequest(content.InvocationRequest, disp, reqMsg.RequestId)
	case *pb.StreamingMessage_FunctionLoadRequest:
		log.Println("Handling FunctionLoadRequest")
		return handleFunctionLoadRequest(content.FunctionLoadRequest, disp, reqMsg.RequestId), nil
	case *pb.StreamingMessage_WorkerStatusRequest:
		log.Println("Handling WorkerStatusRequest")
		return handleWorkerStatusRequest(reqMsg.RequestId, content.WorkerStatusRequest)
	case *pb.StreamingMessage_WorkerTerminate:
		log.Println("Handling WorkerTerminate")
		return handleWorkerTerminate(reqMsg.RequestId, content.WorkerTerminate)
	case *pb.StreamingMessage_FunctionEnvironmentReloadRequest:
		log.Println("Handling FunctionEnvironmentReloadRequest")
		return handleFunctionEnvironmentReloadRequest(reqMsg.RequestId, content.FunctionEnvironmentReloadRequest)
	default:
		log.Printf("Received unhandled message type: %T\n", content)
		return nil, nil
	}
}
