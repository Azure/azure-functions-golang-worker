package worker

import (
	"sync"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

func newTestDispatcher(requestId string) *Dispatcher {
	return &Dispatcher{
		WorkerStartupConfig: &WorkerStartupConfig{
			FunctionsUri:       "127.0.0.1:5000",
			FunctionsWorkerId:  "worker-1",
			FunctionsRequestId: requestId,
		},
		App:             sdk.FunctionApp(),
		LoadedFunctions: &sync.Map{},
	}
}

func TestNewDispatcher(t *testing.T) {
	cfg := &WorkerStartupConfig{
		FunctionsUri:       "127.0.0.1:5000",
		FunctionsWorkerId:  "w-1",
		FunctionsRequestId: "r-1",
	}
	app := sdk.FunctionApp()
	disp := NewDispatcher(cfg, app)

	if disp.WorkerStartupConfig != cfg {
		t.Error("WorkerStartupConfig not set correctly")
	}
	if disp.App != app {
		t.Error("App not set correctly")
	}
	if disp.LoadedFunctions == nil {
		t.Error("LoadedFunctions should not be nil")
	}
}

func TestProcessRequestMessage_WorkerInit(t *testing.T) {
	disp := newTestDispatcher("req-123")

	msg := &pb.StreamingMessage{
		Content: &pb.StreamingMessage_WorkerInitRequest{
			WorkerInitRequest: &pb.WorkerInitRequest{},
		},
	}

	resp, err := disp.processRequestMessage(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected response, got nil")
	}
	if resp.RequestId != "req-123" {
		t.Errorf("expected RequestId %q, got %q", "req-123", resp.RequestId)
	}

	initResp := resp.GetWorkerInitResponse()
	if initResp == nil {
		t.Fatal("expected WorkerInitResponse content")
	}
	if initResp.Result.Status != pb.StatusResult_Success {
		t.Errorf("expected Success status, got %v", initResp.Result.Status)
	}
}

func TestProcessRequestMessage_FunctionsMetadata(t *testing.T) {
	disp := newTestDispatcher("req-meta")

	msg := &pb.StreamingMessage{
		Content: &pb.StreamingMessage_FunctionsMetadataRequest{
			FunctionsMetadataRequest: &pb.FunctionsMetadataRequest{},
		},
	}

	resp, err := disp.processRequestMessage(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.RequestId != "req-meta" {
		t.Errorf("expected RequestId %q, got %q", "req-meta", resp.RequestId)
	}

	metaResp := resp.GetFunctionMetadataResponse()
	if metaResp == nil {
		t.Fatal("expected FunctionMetadataResponse content")
	}
	if metaResp.Result.Status != pb.StatusResult_Success {
		t.Errorf("expected Success status, got %v", metaResp.Result.Status)
	}
}

func TestProcessRequestMessage_WorkerStatus(t *testing.T) {
	disp := newTestDispatcher("req-status")

	msg := &pb.StreamingMessage{
		Content: &pb.StreamingMessage_WorkerStatusRequest{
			WorkerStatusRequest: &pb.WorkerStatusRequest{},
		},
	}

	resp, err := disp.processRequestMessage(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.RequestId != "req-status" {
		t.Errorf("expected RequestId %q, got %q", "req-status", resp.RequestId)
	}
	if resp.GetWorkerStatusResponse() == nil {
		t.Fatal("expected WorkerStatusResponse content")
	}
}

func TestProcessRequestMessage_WorkerTerminate(t *testing.T) {
	disp := newTestDispatcher("req-term")

	msg := &pb.StreamingMessage{
		Content: &pb.StreamingMessage_WorkerTerminate{
			WorkerTerminate: &pb.WorkerTerminate{},
		},
	}

	resp, err := disp.processRequestMessage(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != nil {
		t.Errorf("expected nil response for WorkerTerminate, got %+v", resp)
	}
}

func TestProcessRequestMessage_EnvironmentReload(t *testing.T) {
	disp := newTestDispatcher("req-reload")

	msg := &pb.StreamingMessage{
		Content: &pb.StreamingMessage_FunctionEnvironmentReloadRequest{
			FunctionEnvironmentReloadRequest: &pb.FunctionEnvironmentReloadRequest{},
		},
	}

	resp, err := disp.processRequestMessage(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.RequestId != "req-reload" {
		t.Errorf("expected RequestId %q, got %q", "req-reload", resp.RequestId)
	}

	reloadResp := resp.GetFunctionEnvironmentReloadResponse()
	if reloadResp == nil {
		t.Fatal("expected FunctionEnvironmentReloadResponse content")
	}
	if reloadResp.Result.Status != pb.StatusResult_Success {
		t.Errorf("expected Success status, got %v", reloadResp.Result.Status)
	}
}

func TestProcessRequestMessage_InvocationUsesEnvelopeRequestId(t *testing.T) {
	disp := newTestDispatcher("startup-req-id")

	// Register and load a simple function
	myFunc := func() string { return "hello" }
	app := disp.App
	app.HTTP("test", myFunc)

	// Load all registered functions
	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		funcID := key.(string)
		loadMsg := &pb.StreamingMessage{
			Content: &pb.StreamingMessage_FunctionLoadRequest{
				FunctionLoadRequest: &pb.FunctionLoadRequest{
					FunctionId: funcID,
					Metadata: &pb.RpcFunctionMetadata{
						Name: "test",
					},
				},
			},
		}
		_, _ = disp.processRequestMessage(loadMsg)
		return true
	})

	// Find the registered function ID
	var funcID string
	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		funcID = key.(string)
		return false
	})

	// Send an invocation with a specific envelope RequestId
	invMsg := &pb.StreamingMessage{
		RequestId: "envelope-invocation-request-id",
		Content: &pb.StreamingMessage_InvocationRequest{
			InvocationRequest: &pb.InvocationRequest{
				InvocationId: "inv-1",
				FunctionId:   funcID,
			},
		},
	}

	resp, err := disp.processRequestMessage(invMsg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Invocation responses must use the envelope RequestId, not the startup one
	if resp.RequestId != "envelope-invocation-request-id" {
		t.Errorf("expected invocation response RequestId %q, got %q",
			"envelope-invocation-request-id", resp.RequestId)
	}
}

func TestProcessRequestMessage_UnhandledType(t *testing.T) {
	disp := newTestDispatcher("req-unknown")

	// Use a StartStream message which is client->host, so dispatcher won't handle it
	msg := &pb.StreamingMessage{
		Content: &pb.StreamingMessage_StartStream{
			StartStream: &pb.StartStream{},
		},
	}

	resp, err := disp.processRequestMessage(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != nil {
		t.Errorf("expected nil response for unhandled type, got %+v", resp)
	}
}
