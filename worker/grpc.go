package worker

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func connectToHost(hostAddress string, maxMsgSize int, workerId string) (
	grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage], error) {
	client, err := getBidiStreamClient(hostAddress, maxMsgSize)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC stream: %v", err)
	}

	err = sendStartStreamMessage(client, workerId)
	if err != nil {
		return nil, fmt.Errorf("failed to send start stream message: %v", err)
	}

	return client, nil
}

func getBidiStreamClient(address string, maxMsgSize int) (grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage], error) {
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize), grpc.MaxCallSendMsgSize(maxMsgSize)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return nil, err
	}

	client := pb.NewFunctionRpcClient(conn)

	return client.EventStream(context.Background())
}

// If successful, host is ready to start sending messages to the worker (starting with InitWorkerRequest)
func sendStartStreamMessage(client grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage], workerId string) error {
	startStreamMsg := &pb.StreamingMessage{
		Content: &pb.StreamingMessage_StartStream{
			StartStream: &pb.StartStream{
				WorkerId: workerId,
			},
		},
	}

	return client.Send(startStreamMsg)
}

func handleBidiStream(client grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage], disp *Dispatcher) {
	// gRPC ClientStream.SendMsg is not safe for concurrent use; sendMu
	// serializes Send calls across the per-message dispatch goroutines.
	var sendMu sync.Mutex
	sendResp := func(respMsg *pb.StreamingMessage) {
		sendMu.Lock()
		defer sendMu.Unlock()
		if err := client.Send(respMsg); err != nil {
			log.Printf("Error sending response: %v", err)
		}
	}

	for {
		reqMsg, err := client.Recv()
		if err == io.EOF {
			fmt.Println("Stream closed by server")
			return
		}
		if err != nil {
			log.Fatalf("Error receiving from stream: %v", err)
		}

		// Dispatch every message on its own goroutine. The host correlates
		// responses by function_id / invocation_id / request_id, not by
		// arrival order, so worker-side ordering is irrelevant; control-
		// plane sequencing (init before load, load before invocation,
		// terminate last) is enforced by the host already. Matches the
		// concurrency model used by the Python and .NET-isolated workers.
		//
		// Critically, this keeps the receive loop draining the stream
		// while long-running streaming invocations are in flight, so
		// health pings, env reloads, terminate, and concurrent invocations
		// don't queue behind an SSE / LLM / long-poll handler.
		go func(msg *pb.StreamingMessage) {
			respMsg, err := disp.processRequestMessage(msg)
			if err != nil {
				// Per-message errors must not crash the worker. The host
				// will retry or time out the affected request; other
				// in-flight messages keep flowing.
				log.Printf("Error processing %T: %v", msg.GetContent(), err)
				return
			}
			if respMsg == nil {
				return
			}
			sendResp(respMsg)
		}(reqMsg)
	}
}
