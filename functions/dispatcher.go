package functions

import (
	"fmt"
	"log"

	pb "github.com/azure/azure-functions-golang-worker/proto"
)

type Dispatcher struct {
	WorkerStartupConfig *WorkerStartupConfig
	FunctionRegistry    *FunctionRegistry
}

func NewDispatcher(workerStartupConfig WorkerStartupConfig) *Dispatcher {
	return &Dispatcher{
		WorkerStartupConfig: &workerStartupConfig,
		FunctionRegistry: &FunctionRegistry{
			functions: make(map[string]*FunctionInfo),
		},
	}
}

func (d *Dispatcher) Dispatch(msg *pb.StreamingMessage) (*pb.StreamingMessage, error) {
	if msg == nil {
		return nil, fmt.Errorf("received nil message")
	}

	switch content := msg.Content.(type) {
	case *pb.StreamingMessage_WorkerInitRequest:
		log.Println("Handling WorkerInitRequest")
		return HandleWorkerInitRequest(content.WorkerInitRequest, msg.RequestId), nil
	case *pb.StreamingMessage_FunctionLoadRequest:
		log.Println("Handling FunctionLoadRequest")
		return handleFunctionLoadRequest(msg.RequestId, content.FunctionLoadRequest, d.FunctionRegistry)
	case *pb.StreamingMessage_InvocationRequest:
		log.Println("Handling InvocationRequest")
		return handleInvocationRequest(msg.RequestId, content.InvocationRequest, d.FunctionRegistry)
	case *pb.StreamingMessage_WorkerStatusRequest:
		log.Println("Handling WorkerStatusRequest")
		return handleWorkerStatusRequest(msg.RequestId, content.WorkerStatusRequest)
	case *pb.StreamingMessage_WorkerTerminate:
		log.Println("Handling WorkerTerminate")
		return handleWorkerTerminate(msg.RequestId, content.WorkerTerminate)
	default:
		log.Printf("Received unhandled message type: %T\n", content)
		return nil, nil
	}
}

func ProcessRequstMessage(reqMsg *pb.StreamingMessage) *pb.StreamingMessage {
	switch content := reqMsg.GetContent().(type) {
	case *pb.StreamingMessage_WorkerInitRequest:
		log.Println("Handling WorkerInitRequest")
		return HandleWorkerInitRequest(content.WorkerInitRequest, reqMsg.RequestId)
	default:
		log.Printf("Received unhandled message type: %T\n", content)
		return nil
	}
}
