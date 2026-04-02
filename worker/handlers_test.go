package worker

import (
	"context"
	"net/http"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

func newTestApp() *sdk.App {
	return sdk.FunctionApp()
}

// --- FunctionsMetadataRequest tests ---

func TestHandleFunctionsMetadataRequest_Empty(t *testing.T) {
	app := newTestApp()
	resp := handleFunctionsMetadataRequest(&pb.FunctionsMetadataRequest{}, app, "req-1")

	metaResp := resp.GetContent().(*pb.StreamingMessage_FunctionMetadataResponse).FunctionMetadataResponse
	if len(metaResp.FunctionMetadataResults) != 0 {
		t.Errorf("expected 0 functions, got %d", len(metaResp.FunctionMetadataResults))
	}
}

func TestHandleFunctionsMetadataRequest_WithFunction(t *testing.T) {
	app := newTestApp()
	app.HTTP("hello", sdk.HTTPHandler(func(w http.ResponseWriter, r *http.Request) {}),
		sdk.WithMethods("GET"), sdk.WithAuth("anonymous"))

	resp := handleFunctionsMetadataRequest(&pb.FunctionsMetadataRequest{}, app, "req-1")
	metaResp := resp.GetContent().(*pb.StreamingMessage_FunctionMetadataResponse).FunctionMetadataResponse

	if len(metaResp.FunctionMetadataResults) != 1 {
		t.Fatalf("expected 1 function, got %d", len(metaResp.FunctionMetadataResults))
	}

	meta := metaResp.FunctionMetadataResults[0]
	if meta.Language != "go" {
		t.Errorf("expected language %q, got %q", "go", meta.Language)
	}
}

// --- InvocationRequest tests ---

func TestHandleInvocationRequest_FunctionNotLoaded(t *testing.T) {
	app := newTestApp()
	disp := newTestDispatcher("req-1")
	disp.App = app

	_, err := handleInvocationRequest(&pb.InvocationRequest{
		FunctionId:   "nonexistent",
		InvocationId: "inv-1",
	}, disp, "req-1")

	if err == nil {
		t.Fatal("expected error for unloaded function")
	}
}

func TestHandleInvocationRequest_SimpleHTTP(t *testing.T) {
	app := newTestApp()
	disp := newTestDispatcher("req-1")
	disp.App = app

	handler := sdk.HTTPHandler(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello"))
	})
	app.HTTP("test", handler, sdk.WithMethods("GET"))

	// Get function ID
	var funcID string
	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		funcID = key.(string)
		return false
	})

	// Load the function
	loadResp := handleFunctionLoadRequest(&pb.FunctionLoadRequest{
		FunctionId: funcID,
	}, disp, "req-1")
	loadResult := loadResp.GetContent().(*pb.StreamingMessage_FunctionLoadResponse).FunctionLoadResponse
	if loadResult.Result.Status != pb.StatusResult_Success {
		t.Fatalf("expected load success, got %v", loadResult.Result.Status)
	}

	// Invoke
	resp, err := handleInvocationRequest(&pb.InvocationRequest{
		FunctionId:   funcID,
		InvocationId: "inv-1",
		InputData: []*pb.ParameterBinding{
			{
				Name: "req",
				RpcData: &pb.ParameterBinding_Data{
					Data: &pb.TypedData{
						Data: &pb.TypedData_Http{
							Http: &pb.RpcHttp{
								Method: "GET",
								Url:    "http://localhost/api/test",
							},
						},
					},
				},
			},
		},
	}, disp, "req-1")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	invResp := resp.GetContent().(*pb.StreamingMessage_InvocationResponse).InvocationResponse
	if invResp.Result.Status != pb.StatusResult_Success {
		t.Errorf("expected Success, got %v: %v", invResp.Result.Status, invResp.Result.Exception)
	}
}

func TestHandleInvocationRequest_TimerWithError(t *testing.T) {
	app := newTestApp()
	disp := newTestDispatcher("req-1")
	disp.App = app

	handler := sdk.TimerHandler(func(ctx context.Context, timer bindings.TimerInfo) error {
		return context.DeadlineExceeded
	})
	app.Timer("tick", handler, sdk.WithSchedule("0 * * * * *"))

	var funcID string
	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		funcID = key.(string)
		return false
	})

	handleFunctionLoadRequest(&pb.FunctionLoadRequest{FunctionId: funcID}, disp, "req-1")

	resp, err := handleInvocationRequest(&pb.InvocationRequest{
		FunctionId:   funcID,
		InvocationId: "inv-1",
		InputData: []*pb.ParameterBinding{
			{
				Name: "timer",
				RpcData: &pb.ParameterBinding_Data{
					Data: &pb.TypedData{
						Data: &pb.TypedData_Json{Json: `{"schedule":{},"scheduleStatus":{"last":"","next":""},"isPastDue":false}`},
					},
				},
			},
		},
	}, disp, "req-1")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	invResp := resp.GetContent().(*pb.StreamingMessage_InvocationResponse).InvocationResponse
	if invResp.Result.Status != pb.StatusResult_Failure {
		t.Errorf("expected Failure, got %v", invResp.Result.Status)
	}
}

// --- ServiceBus trigger metadata test ---

func TestHandleFunctionsMetadataRequest_ServiceBus(t *testing.T) {
	app := newTestApp()
	app.ServiceBusQueue("sbFunc", sdk.ServiceBusHandler(func(ctx context.Context, msg bindings.ServiceBusMessage) error {
		return nil
	}), sdk.WithQueueName("myqueue"), sdk.WithConnection("SBConn"))

	resp := handleFunctionsMetadataRequest(&pb.FunctionsMetadataRequest{}, app, "req-1")
	metaResp := resp.GetContent().(*pb.StreamingMessage_FunctionMetadataResponse).FunctionMetadataResponse

	if len(metaResp.FunctionMetadataResults) != 1 {
		t.Fatalf("expected 1 function, got %d", len(metaResp.FunctionMetadataResults))
	}

	meta := metaResp.FunctionMetadataResults[0]
	triggerBinding, ok := meta.Bindings["message"]
	if !ok {
		t.Fatal("expected 'message' binding")
	}
	if triggerBinding.Type != "serviceBusTrigger" {
		t.Errorf("expected type %q, got %q", "serviceBusTrigger", triggerBinding.Type)
	}
	if triggerBinding.Direction != pb.BindingInfo_in {
		t.Errorf("expected direction 'in', got %v", triggerBinding.Direction)
	}
}
