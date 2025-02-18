package internal

import (
	"fmt"
	"log"

	functionrpc "github.com/azure/azure-functions-golang-worker/proto"
)

// Dispatcher is responsible for routing incoming gRPC StreamingMessages to the appropriate handler.
type Dispatcher struct {
	// Optionally store references to any shared data structures, like a function registry.
	functionRegistry *FunctionRegistry
}

// NewDispatcher creates and returns a Dispatcher with any required dependencies.
func NewDispatcher(registry *FunctionRegistry) *Dispatcher {
	return &Dispatcher{
		functionRegistry: registry,
	}
}

// Dispatch looks at the oneof content of the StreamingMessage
// and calls the matching handler. It returns an optional response
// StreamingMessage (or nil) and an error if something went wrong.
func (d *Dispatcher) Dispatch(msg *functionrpc.StreamingMessage) (*functionrpc.StreamingMessage, error) {
	if msg == nil {
		return nil, fmt.Errorf("received nil message")
	}

	switch content := msg.Content.(type) {

	// -----------------------------------------
	// Example: Worker Init
	case *functionrpc.StreamingMessage_WorkerInitRequest:
		log.Println("Handling WorkerInitRequest")
		return handleWorkerInitRequest(msg.RequestId, content.WorkerInitRequest)

	// -----------------------------------------
	// Example: Function Load
	case *functionrpc.StreamingMessage_FunctionLoadRequest:
		log.Println("Handling FunctionLoadRequest")
		return handleFunctionLoadRequest(msg.RequestId, content.FunctionLoadRequest, d.functionRegistry)

	// -----------------------------------------
	// Example: Invocation
	case *functionrpc.StreamingMessage_InvocationRequest:
		log.Println("Handling InvocationRequest")
		return handleInvocationRequest(msg.RequestId, content.InvocationRequest, d.functionRegistry)

	// -----------------------------------------
	// Example: Worker Status
	case *functionrpc.StreamingMessage_WorkerStatusRequest:
		log.Println("Handling WorkerStatusRequest")
		return handleWorkerStatusRequest(msg.RequestId, content.WorkerStatusRequest)

	// -----------------------------------------
	// Example: Worker Terminate
	case *functionrpc.StreamingMessage_WorkerTerminate:
		log.Println("Handling WorkerTerminate")
		return handleWorkerTerminate(msg.RequestId, content.WorkerTerminate)

	// -----------------------------------------
	// Catch-all for unimplemented message types
	default:
		log.Printf("Received unhandled message type: %T\n", content)
		// You could return an error or simply return nil.
		// For now, we’ll just return nil indicating no response needed.
		return nil, nil
	}
}
