package worker

import (
	"context"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

func TestHandleInvocationRequest_EventHubBatch(t *testing.T) {
	var got []bindings.EventHubMessage
	app := newTestApp()
	dispatcher := newTestDispatcher("req-eventhub-batch")
	dispatcher.App = app
	registered := app.EventHub("events", sdk.EventHubBatchHandler(
		func(ctx context.Context, events []bindings.EventHubMessage) error {
			got = events
			return nil
		},
	))

	handleFunctionLoadRequest(&pb.FunctionLoadRequest{FunctionId: registered.FuncId}, dispatcher, "req-eventhub-batch")
	response, err := handleInvocationRequest(batchInvocationRequest(
		registered.FuncId,
		&pb.TypedData{Data: &pb.TypedData_CollectionBytes{
			CollectionBytes: &pb.CollectionBytes{Bytes: [][]byte{[]byte("first"), []byte("second")}},
		}},
		map[string]*pb.TypedData{
			"SequenceNumberArray": {
				Data: &pb.TypedData_CollectionSint64{
					CollectionSint64: &pb.CollectionSInt64{Sint64: []int64{1, 2}},
				},
			},
		},
	), dispatcher, "req-eventhub-batch")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	invocation := response.GetContent().(*pb.StreamingMessage_InvocationResponse).InvocationResponse
	if invocation.Result.Status != pb.StatusResult_Success {
		t.Fatalf("expected success, got %v", invocation.Result.Status)
	}
	if len(got) != 2 || string(got[1].Body) != "second" || got[1].SequenceNumber != 2 {
		t.Fatalf("unexpected events: %+v", got)
	}
}

func TestHandleInvocationRequest_ServiceBusBatch(t *testing.T) {
	var got []bindings.ServiceBusMessage
	app := newTestApp()
	dispatcher := newTestDispatcher("req-servicebus-batch")
	dispatcher.App = app
	registered := app.ServiceBusQueue("messages", sdk.ServiceBusBatchHandler(
		func(ctx context.Context, messages []bindings.ServiceBusMessage) error {
			got = messages
			return nil
		},
	))

	handleFunctionLoadRequest(&pb.FunctionLoadRequest{FunctionId: registered.FuncId}, dispatcher, "req-servicebus-batch")
	response, err := handleInvocationRequest(batchInvocationRequest(
		registered.FuncId,
		&pb.TypedData{Data: &pb.TypedData_CollectionString{
			CollectionString: &pb.CollectionString{String_: []string{"first", "second"}},
		}},
		map[string]*pb.TypedData{
			"MessageIdArray": {
				Data: &pb.TypedData_CollectionString{
					CollectionString: &pb.CollectionString{String_: []string{"id-1", "id-2"}},
				},
			},
		},
	), dispatcher, "req-servicebus-batch")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	invocation := response.GetContent().(*pb.StreamingMessage_InvocationResponse).InvocationResponse
	if invocation.Result.Status != pb.StatusResult_Success {
		t.Fatalf("expected success, got %v", invocation.Result.Status)
	}
	if len(got) != 2 || string(got[1].Body) != "second" || got[1].MessageId != "id-2" {
		t.Fatalf("unexpected messages: %+v", got)
	}
}

func batchInvocationRequest(functionID string, data *pb.TypedData, metadata map[string]*pb.TypedData) *pb.InvocationRequest {
	return &pb.InvocationRequest{
		FunctionId:      functionID,
		InvocationId:    "inv-batch",
		TriggerMetadata: metadata,
		InputData: []*pb.ParameterBinding{
			{
				Name: "message",
				RpcData: &pb.ParameterBinding_Data{
					Data: data,
				},
			},
		},
	}
}
