package worker

import (
	"context"
	"fmt"
	"io"
	"log"

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
	for {
		reqMsg, err := client.Recv()
		if err == io.EOF {
			fmt.Println("Stream closed by server")
			return
		}
		if err != nil {
			log.Fatalf("Error receiving from stream: %v", err)
		}

		respMsg, err := disp.processRequestMessage(reqMsg)
		if err != nil {
			log.Fatalf("Error processing request: %v", err)
		} else if respMsg == nil {
			// fmt.Println("Warning: ProcessRequstMessage returned nil, no response will be sent.")
			continue
		}

		// fmt.Printf("Sending response: %v\n", respMsg)
		if err := client.Send(respMsg); err != nil {
			log.Fatalf("Error sending response: %v", err)
		}
	}
}
