package worker

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

func TestHandleWorkerInitRequest(t *testing.T) {
	resp := handleWorkerInitRequest(&pb.WorkerInitRequest{}, "req-init-1")

	if resp.RequestId != "req-init-1" {
		t.Errorf("expected RequestId %q, got %q", "req-init-1", resp.RequestId)
	}

	initResp := resp.GetWorkerInitResponse()
	if initResp == nil {
		t.Fatal("expected WorkerInitResponse content")
	}
	if initResp.Result.Status != pb.StatusResult_Success {
		t.Errorf("expected Success status, got %v", initResp.Result.Status)
	}
	if initResp.WorkerVersion != "1.0.0" {
		t.Errorf("expected worker version %q, got %q", "1.0.0", initResp.WorkerVersion)
	}
}

func TestHandleFunctionsMetadataRequest_EmptyApp(t *testing.T) {
	app := sdk.FunctionApp()
	resp := handleFunctionsMetadataRequest(&pb.FunctionsMetadataRequest{}, app, "req-meta-1")

	if resp.RequestId != "req-meta-1" {
		t.Errorf("expected RequestId %q, got %q", "req-meta-1", resp.RequestId)
	}

	metaResp := resp.GetFunctionMetadataResponse()
	if metaResp == nil {
		t.Fatal("expected FunctionMetadataResponse content")
	}
	if metaResp.Result.Status != pb.StatusResult_Success {
		t.Errorf("expected Success, got %v", metaResp.Result.Status)
	}
	if len(metaResp.FunctionMetadataResults) != 0 {
		t.Errorf("expected 0 functions, got %d", len(metaResp.FunctionMetadataResults))
	}
}

func TestHandleFunctionsMetadataRequest_WithHTTPFunction(t *testing.T) {
	app := sdk.FunctionApp()
	handler := func(req *http.Request) {}
	app.HTTP("hello", handler)

	resp := handleFunctionsMetadataRequest(&pb.FunctionsMetadataRequest{}, app, "req-meta-2")
	metaResp := resp.GetFunctionMetadataResponse()

	if len(metaResp.FunctionMetadataResults) != 1 {
		t.Fatalf("expected 1 function, got %d", len(metaResp.FunctionMetadataResults))
	}

	meta := metaResp.FunctionMetadataResults[0]
	if meta.Language != "go" {
		t.Errorf("expected language %q, got %q", "go", meta.Language)
	}

	// Should have bindings: "req" (in) and "$return" (out)
	if len(meta.Bindings) < 2 {
		t.Errorf("expected at least 2 bindings, got %d", len(meta.Bindings))
	}

	reqBinding, ok := meta.Bindings["req"]
	if !ok {
		t.Fatal("expected 'req' binding")
	}
	if reqBinding.Type != "httpTrigger" {
		t.Errorf("expected binding type %q, got %q", "httpTrigger", reqBinding.Type)
	}
	if reqBinding.Direction != pb.BindingInfo_in {
		t.Errorf("expected direction 'in', got %v", reqBinding.Direction)
	}

	retBinding, ok := meta.Bindings["$return"]
	if !ok {
		t.Fatal("expected '$return' binding")
	}
	if retBinding.Direction != pb.BindingInfo_out {
		t.Errorf("expected direction 'out', got %v", retBinding.Direction)
	}
}

func TestHandleFunctionLoadRequest_NotFound(t *testing.T) {
	disp := &Dispatcher{
		WorkerStartupConfig: &WorkerStartupConfig{FunctionsRequestId: "req-load-1"},
		App:                 sdk.FunctionApp(),
		LoadedFunctions:     &sync.Map{},
	}

	resp := handleFunctionLoadRequest(&pb.FunctionLoadRequest{
		FunctionId: "nonexistent-id",
	}, disp, "req-load-1")

	loadResp := resp.GetFunctionLoadResponse()
	if loadResp == nil {
		t.Fatal("expected FunctionLoadResponse content")
	}
	if loadResp.Result.Status != pb.StatusResult_Failure {
		t.Errorf("expected Failure status, got %v", loadResp.Result.Status)
	}
	if loadResp.FunctionId != "nonexistent-id" {
		t.Errorf("expected FunctionId %q, got %q", "nonexistent-id", loadResp.FunctionId)
	}
}

func TestHandleFunctionLoadRequest_Success(t *testing.T) {
	app := sdk.FunctionApp()
	handler := func(ctx context.Context, req *http.Request) string { return "ok" }
	app.HTTP("testload", handler)

	disp := &Dispatcher{
		WorkerStartupConfig: &WorkerStartupConfig{FunctionsRequestId: "req-load-2"},
		App:                 app,
		LoadedFunctions:     &sync.Map{},
	}

	// Get the registered function ID
	var funcID string
	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		funcID = key.(string)
		return false
	})

	resp := handleFunctionLoadRequest(&pb.FunctionLoadRequest{
		FunctionId: funcID,
		Metadata:   &pb.RpcFunctionMetadata{Name: "testload"},
	}, disp, "req-load-2")

	loadResp := resp.GetFunctionLoadResponse()
	if loadResp == nil {
		t.Fatal("expected FunctionLoadResponse content")
	}
	if loadResp.Result.Status != pb.StatusResult_Success {
		t.Errorf("expected Success status, got %v", loadResp.Result.Status)
	}

	// Verify function was stored in LoadedFunctions
	_, ok := disp.LoadedFunctions.Load(funcID)
	if !ok {
		t.Error("expected function to be stored in LoadedFunctions")
	}
}

func TestHandleFunctionLoadRequest_WithResponseWriter(t *testing.T) {
	app := sdk.FunctionApp()
	handler := func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("hello"))
	}
	app.HTTP("testwriter", handler)

	disp := &Dispatcher{
		WorkerStartupConfig: &WorkerStartupConfig{FunctionsRequestId: "req-load-3"},
		App:                 app,
		LoadedFunctions:     &sync.Map{},
	}

	var funcID string
	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		funcID = key.(string)
		return false
	})

	resp := handleFunctionLoadRequest(&pb.FunctionLoadRequest{
		FunctionId: funcID,
		Metadata:   &pb.RpcFunctionMetadata{Name: "testwriter"},
	}, disp, "req-load-3")

	loadResp := resp.GetFunctionLoadResponse()
	if loadResp.Result.Status != pb.StatusResult_Success {
		t.Errorf("expected Success, got %v", loadResp.Result.Status)
	}

	// Verify $return field was mapped to the writer
	val, ok := disp.LoadedFunctions.Load(funcID)
	if !ok {
		t.Fatal("function not loaded")
	}
	lf := val.(*LoadedFunction)
	retField, ok := lf.Fields["$return"]
	if !ok {
		t.Fatal("expected '$return' field mapping")
	}
	if !retField.IsWriter {
		t.Error("expected $return to be marked as writer")
	}
}

func TestHandleInvocationRequest_FunctionNotLoaded(t *testing.T) {
	disp := &Dispatcher{
		LoadedFunctions: &sync.Map{},
	}

	_, err := handleInvocationRequest(&pb.InvocationRequest{
		FunctionId:   "missing-func",
		InvocationId: "inv-1",
	}, disp, "req-inv-1")

	if err == nil {
		t.Fatal("expected error for missing function")
	}
}

func TestHandleInvocationRequest_SimpleFunction(t *testing.T) {
	app := sdk.FunctionApp()
	handler := func() string { return "hello world" }
	app.HTTP("simplefunc", handler)

	disp := &Dispatcher{
		WorkerStartupConfig: &WorkerStartupConfig{FunctionsRequestId: "startup-req"},
		App:                 app,
		LoadedFunctions:     &sync.Map{},
	}

	// Load the function first
	var funcID string
	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		funcID = key.(string)
		return false
	})

	handleFunctionLoadRequest(&pb.FunctionLoadRequest{
		FunctionId: funcID,
		Metadata:   &pb.RpcFunctionMetadata{Name: "simplefunc"},
	}, disp, "startup-req")

	resp, err := handleInvocationRequest(&pb.InvocationRequest{
		FunctionId:   funcID,
		InvocationId: "inv-simple",
	}, disp, "req-inv-simple")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	invResp := resp.GetInvocationResponse()
	if invResp == nil {
		t.Fatal("expected InvocationResponse content")
	}
	if invResp.InvocationId != "inv-simple" {
		t.Errorf("expected InvocationId %q, got %q", "inv-simple", invResp.InvocationId)
	}
	if invResp.Result.Status != pb.StatusResult_Success {
		t.Errorf("expected Success, got %v", invResp.Result.Status)
	}
	if resp.RequestId != "req-inv-simple" {
		t.Errorf("expected RequestId %q, got %q", "req-inv-simple", resp.RequestId)
	}
}

func TestHandleInvocationRequest_FunctionReturnsError(t *testing.T) {
	app := sdk.FunctionApp()
	handler := func() (string, error) {
		return "", fmt.Errorf("something went wrong")
	}
	app.HTTP("errfunc", handler)

	disp := &Dispatcher{
		WorkerStartupConfig: &WorkerStartupConfig{FunctionsRequestId: "startup-req"},
		App:                 app,
		LoadedFunctions:     &sync.Map{},
	}

	var funcID string
	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		funcID = key.(string)
		return false
	})

	handleFunctionLoadRequest(&pb.FunctionLoadRequest{
		FunctionId: funcID,
		Metadata:   &pb.RpcFunctionMetadata{Name: "errfunc"},
	}, disp, "startup-req")

	resp, err := handleInvocationRequest(&pb.InvocationRequest{
		FunctionId:   funcID,
		InvocationId: "inv-err",
	}, disp, "req-inv-err")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	invResp := resp.GetInvocationResponse()
	if invResp.Result.Status != pb.StatusResult_Failure {
		t.Errorf("expected Failure status, got %v", invResp.Result.Status)
	}
	if invResp.Result.Exception == nil {
		t.Fatal("expected exception in result")
	}
	if invResp.Result.Exception.Message != "something went wrong" {
		t.Errorf("expected exception message %q, got %q", "something went wrong", invResp.Result.Exception.Message)
	}
}

func TestHandleInvocationRequest_WithHTTPInput(t *testing.T) {
	app := sdk.FunctionApp()
	handler := func(req *http.Request) string {
		return "got: " + req.Method
	}
	app.HTTP("httpfunc", handler)

	disp := &Dispatcher{
		WorkerStartupConfig: &WorkerStartupConfig{FunctionsRequestId: "startup-req"},
		App:                 app,
		LoadedFunctions:     &sync.Map{},
	}

	var funcID string
	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		funcID = key.(string)
		return false
	})

	handleFunctionLoadRequest(&pb.FunctionLoadRequest{
		FunctionId: funcID,
		Metadata:   &pb.RpcFunctionMetadata{Name: "httpfunc"},
	}, disp, "startup-req")

	resp, err := handleInvocationRequest(&pb.InvocationRequest{
		FunctionId:   funcID,
		InvocationId: "inv-http",
		InputData: []*pb.ParameterBinding{
			{
				Name: "req",
				RpcData: &pb.ParameterBinding_Data{
					Data: &pb.TypedData{
						Data: &pb.TypedData_Http{
							Http: &pb.RpcHttp{
								Method: "POST",
								Url:    "http://localhost/api/httpfunc",
							},
						},
					},
				},
			},
		},
	}, disp, "req-inv-http")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	invResp := resp.GetInvocationResponse()
	if invResp.Result.Status != pb.StatusResult_Success {
		t.Errorf("expected Success, got %v", invResp.Result.Status)
	}
}

func TestHandleInvocationRequest_WithResponseWriter(t *testing.T) {
	app := sdk.FunctionApp()
	handler := func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("created"))
	}
	app.HTTP("writerfunc", handler)

	disp := &Dispatcher{
		WorkerStartupConfig: &WorkerStartupConfig{FunctionsRequestId: "startup-req"},
		App:                 app,
		LoadedFunctions:     &sync.Map{},
	}

	var funcID string
	app.RegisteredFunctions.Range(func(key, value interface{}) bool {
		funcID = key.(string)
		return false
	})

	handleFunctionLoadRequest(&pb.FunctionLoadRequest{
		FunctionId: funcID,
		Metadata:   &pb.RpcFunctionMetadata{Name: "writerfunc"},
	}, disp, "startup-req")

	resp, err := handleInvocationRequest(&pb.InvocationRequest{
		FunctionId:   funcID,
		InvocationId: "inv-writer",
		InputData: []*pb.ParameterBinding{
			{
				Name: "req",
				RpcData: &pb.ParameterBinding_Data{
					Data: &pb.TypedData{
						Data: &pb.TypedData_Http{
							Http: &pb.RpcHttp{
								Method: "POST",
								Url:    "http://localhost/api/writerfunc",
							},
						},
					},
				},
			},
		},
	}, disp, "req-inv-writer")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	invResp := resp.GetInvocationResponse()
	if invResp.Result.Status != pb.StatusResult_Success {
		t.Errorf("expected Success, got %v", invResp.Result.Status)
	}

	// Should have return value (HTTP response from writer)
	if invResp.ReturnValue == nil {
		t.Fatal("expected ReturnValue for writer-based function")
	}
	httpData := invResp.ReturnValue.GetHttp()
	if httpData == nil {
		t.Fatal("expected HTTP return value")
	}
	if httpData.StatusCode != "201" {
		t.Errorf("expected status code %q, got %q", "201", httpData.StatusCode)
	}
}

func TestHandleWorkerStatusRequest(t *testing.T) {
	resp, err := handleWorkerStatusRequest("req-ws", &pb.WorkerStatusRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.RequestId != "req-ws" {
		t.Errorf("expected RequestId %q, got %q", "req-ws", resp.RequestId)
	}
	if resp.GetWorkerStatusResponse() == nil {
		t.Fatal("expected WorkerStatusResponse content")
	}
}

func TestHandleWorkerTerminate(t *testing.T) {
	resp, err := handleWorkerTerminate("req-wt", &pb.WorkerTerminate{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != nil {
		t.Errorf("expected nil response for terminate, got %+v", resp)
	}
}

func TestHandleFunctionEnvironmentReloadRequest(t *testing.T) {
	resp, err := handleFunctionEnvironmentReloadRequest("req-env", &pb.FunctionEnvironmentReloadRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.RequestId != "req-env" {
		t.Errorf("expected RequestId %q, got %q", "req-env", resp.RequestId)
	}

	reloadResp := resp.GetFunctionEnvironmentReloadResponse()
	if reloadResp == nil {
		t.Fatal("expected FunctionEnvironmentReloadResponse content")
	}
	if reloadResp.Result.Status != pb.StatusResult_Success {
		t.Errorf("expected Success, got %v", reloadResp.Result.Status)
	}
}

func TestHandleFunctionsMetadataRequest_BindingDirections(t *testing.T) {
	app := sdk.FunctionApp()

	// Register a function that produces bindings with different directions
	trigger := &bindings.SimpleBinding{
		Name:      "input",
		Type:      "customTrigger",
		Direction: "in",
	}
	handler := func(data string) string { return data }
	app.RegisterFunction(handler, trigger)

	resp := handleFunctionsMetadataRequest(&pb.FunctionsMetadataRequest{}, app, "req-dirs")
	metaResp := resp.GetFunctionMetadataResponse()

	if len(metaResp.FunctionMetadataResults) != 1 {
		t.Fatalf("expected 1 function, got %d", len(metaResp.FunctionMetadataResults))
	}

	meta := metaResp.FunctionMetadataResults[0]
	inputBinding, ok := meta.Bindings["input"]
	if !ok {
		t.Fatal("expected 'input' binding")
	}
	if inputBinding.Direction != pb.BindingInfo_in {
		t.Errorf("expected direction 'in', got %v", inputBinding.Direction)
	}
}
