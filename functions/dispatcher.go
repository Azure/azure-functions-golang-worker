package functions

import (
	"fmt"
	"log"
	"net"

	functionrpc "github.com/azure/azure-functions-golang-worker/proto"
	"google.golang.org/grpc"
)

type Dispatcher struct {
	CmdLineArgs      *CmdLineArgs
	FunctionRegistry *FunctionRegistry
}

func NewDispatcher(cmdLineArgs CmdLineArgs) *Dispatcher {
	return &Dispatcher{
		CmdLineArgs: &cmdLineArgs,
		FunctionRegistry: &FunctionRegistry{
			functions: make(map[string]*FunctionInfo),
		},
	}
}

func (d *Dispatcher) Dispatch(msg *functionrpc.StreamingMessage) (*functionrpc.StreamingMessage, error) {
	if msg == nil {
		return nil, fmt.Errorf("received nil message")
	}

	switch content := msg.Content.(type) {
	case *functionrpc.StreamingMessage_WorkerInitRequest:
		log.Println("Handling WorkerInitRequest")
		return handleWorkerInitRequest(msg.RequestId, content.WorkerInitRequest)
	case *functionrpc.StreamingMessage_FunctionLoadRequest:
		log.Println("Handling FunctionLoadRequest")
		return handleFunctionLoadRequest(msg.RequestId, content.FunctionLoadRequest, d.FunctionRegistry)
	case *functionrpc.StreamingMessage_InvocationRequest:
		log.Println("Handling InvocationRequest")
		return handleInvocationRequest(msg.RequestId, content.InvocationRequest, d.FunctionRegistry)
	case *functionrpc.StreamingMessage_WorkerStatusRequest:
		log.Println("Handling WorkerStatusRequest")
		return handleWorkerStatusRequest(msg.RequestId, content.WorkerStatusRequest)
	case *functionrpc.StreamingMessage_WorkerTerminate:
		log.Println("Handling WorkerTerminate")
		return handleWorkerTerminate(msg.RequestId, content.WorkerTerminate)
	default:
		log.Printf("Received unhandled message type: %T\n", content)
		return nil, nil
	}
}

func (dispatcher *Dispatcher) StartWorkerServer() error {
	grpcServer := grpc.NewServer()
	workerServer := NewWorkerServer(dispatcher)
	functionrpc.RegisterFunctionRpcServer(grpcServer, workerServer)

	address := dispatcher.CmdLineArgs.HostAddress
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", address, err)
	}

	log.Printf("Starting Azure Function Go worker.")
	log.Printf("Host Address=%s, Request ID=%s, Worker ID=%s, grpc-max-message-length=%d\n",
		address, dispatcher.CmdLineArgs.HostRequestId, dispatcher.CmdLineArgs.WorkerId, dispatcher.CmdLineArgs.FunctionsGrpcMaxMessageLength)

	return grpcServer.Serve(lis)
}
