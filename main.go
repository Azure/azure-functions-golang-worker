package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/azure/azure-functions-golang-worker/functions"
	pb "github.com/azure/azure-functions-golang-worker/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// type Document struct {
// 	ID    string
// 	Items string
// 	Rid   string
// 	Etag  string
// }

// func cosmosDBFunction(doc Document) Document {
// 	return doc
// }

func main() {
	// Create the app/handler

	app := functions.FunctionApp()
	address := strings.TrimPrefix(app.WorkerStartupConfig.FunctionsUri, "http://")
	address = strings.TrimSuffix(address, "/")
	maxMsgSize := app.WorkerStartupConfig.FunctionsGrpcMaxMessageLength

	conn, err := grpc.NewClient(address,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMsgSize),
			grpc.MaxCallSendMsgSize(maxMsgSize),
		), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Error making channel: %v", err)
	}
	defer conn.Close()

	client := pb.NewFunctionRpcClient(conn)
	stream, err := client.EventStream(context.Background())
	if err != nil {
		log.Fatalf("Error streaming: %v. The functions URI here is: %s", err, address)
	}

	//waitc := make(chan struct{})
	// go func() {
	// 	for {
	// 		_, err := stream.Recv()
	// 		if err == io.EOF {
	// 			// read done.
	// 			close(waitc)
	// 			return
	// 		}
	// 		if err != nil {
	// 			log.Fatalf("Failed to receive stream : %v", err)
	// 		}
	// 	}
	// }()

	startStreamMsg := &pb.StreamingMessage{
		RequestId: app.WorkerStartupConfig.FunctionsRequestId,
		Content: &pb.StreamingMessage_StartStream{
			StartStream: &pb.StartStream{
				WorkerId: app.WorkerStartupConfig.FunctionsWorkerId,
			},
		},
	}

	err = stream.Send(startStreamMsg)
	if err != nil {
		log.Fatalf("Error sending start stream message: %v", err)
	}
	time.Sleep(15 * time.Second)

	msg, err := stream.Recv()
	if err != nil {
		log.Fatalf("Error sending start stream message: %v", err)
	}
	if msg == nil {
		log.Fatalf("Received nil message")
		os.Exit(3)
	}
	time.Sleep(15 * time.Second)

	switch content := msg.GetContent().(type) {
	case *pb.StreamingMessage_WorkerInitRequest:
		log.Println("Handling WorkerInitRequest")
		initResponseMsg := &pb.StreamingMessage{
			RequestId: app.WorkerStartupConfig.FunctionsRequestId,
			Content: &pb.StreamingMessage_WorkerInitResponse{
				WorkerInitResponse: &pb.WorkerInitResponse{
					Result: &pb.StatusResult{
						Status: pb.StatusResult_Success,
					},
					WorkerVersion: content.WorkerInitRequest.FunctionAppDirectory,
					// Optionally fill other fields like capabilities, etc.
				},
			},
		}

		err = stream.Send(initResponseMsg)
		if err != nil {
			log.Fatalf("Error sending init response message: %v", err)
			os.Exit(3)
		}
	}
	time.Sleep(45 * time.Second)

	// Register function(s)
	// app.RegisterCosmosFunction(cosmosDBFunction)
	// app.RegisterCosmosFunction(cosmosDBFunction, connectionStringToCosmos)
	// app.RegisterHttpFunction(httpFunction)
	// app.RegisterBlobFunction(blobFunction)
	// app.RegisterQueueFunction(queueFunction)
}
