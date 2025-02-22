package functions

import (
	"context"
	"io"
	"os"

	pb "github.com/azure/azure-functions-golang-worker/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func CreateGrpcClientConnection(address string, maxMsgSize int) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize), grpc.MaxCallSendMsgSize(maxMsgSize)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.NewClient(address, opts...)

	return conn, err
}

func ConnectToGrpcStream(conn *grpc.ClientConn) (grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage], error) {
	client := pb.NewFunctionRpcClient(conn)
	return client.EventStream(context.Background())
}

// If successful, host is ready to start sending messages to the worker (starting with InitWorkerRequest)
func SendStartStreamMessage(stream grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage], workerId string) error {
	startStreamMsg := &pb.StreamingMessage{
		RequestId: "abc123",
		Content: &pb.StreamingMessage_StartStream{
			StartStream: &pb.StartStream{
				WorkerId: workerId,
			},
		},
	}

	return stream.Send(startStreamMsg)
}

func StartBackgroundStreamReader(stream grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage]) {
	waitc := make(chan *pb.StreamingMessage)
	go func() {
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				// Read done
				os.Exit(1)
				close(waitc)
				return
			}
			if err != nil {
				os.Exit(12)
				// log.Fatalf("Failed to receive stream: %v", err)
			}
			waitc <- msg // Send message to channel
		}
	}()

	go func() {
		for msg := range waitc {
			if respMsg := ProcessRequstMessage(msg); respMsg != nil {
				if err := stream.Send(respMsg); err != nil {
					os.Exit(3)
					//log.Fatalf("Failed to send response: %v", err)
				}
			}
		}
	}()
}
