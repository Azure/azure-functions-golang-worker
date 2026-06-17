package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// changesPayload is the wire-format example shape used by the
// Microsoft.Azure.WebJobs.Extensions.Sql host extension when delivering
// change batches. Operation: 0=Insert, 1=Update, 2=Delete.
const changesPayload = `[` +
	`{"Operation":0,"Item":{"ProductId":1,"Name":"Widget","Cost":100}},` +
	`{"Operation":1,"Item":{"ProductId":2,"Name":"Gadget","Cost":250}},` +
	`{"Operation":2,"Item":{"ProductId":3,"Name":"Gizmo","Cost":50}}` +
	`]`

func runSQLInvocation(t *testing.T, payload *pb.TypedData,
	handler sdk.SQLChangeHandler) *pb.InvocationResponse {
	t.Helper()

	app := newTestApp()
	disp := newTestDispatcher("req-sql")
	disp.App = app

	app.SQL("productsChanged", handler,
		sdk.WithTable("dbo.Products"),
		sdk.WithConnection("AzureWebJobsSqlConnectionString"),
	)

	var funcID string
	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		funcID = key.(string)
		return false
	})

	handleFunctionLoadRequest(&pb.FunctionLoadRequest{FunctionId: funcID}, disp, "req-sql")

	resp, err := handleInvocationRequest(&pb.InvocationRequest{
		FunctionId:   funcID,
		InvocationId: "inv-1",
		InputData: []*pb.ParameterBinding{
			{
				Name: "changes",
				RpcData: &pb.ParameterBinding_Data{
					Data: payload,
				},
			},
		},
	}, disp, "req-sql")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	return resp.GetContent().(*pb.StreamingMessage_InvocationResponse).InvocationResponse
}

func TestHandleInvocationRequest_SQLChanges_Insert(t *testing.T) {
	var got []bindings.SQLChange
	handler := sdk.SQLChangeHandler(func(ctx context.Context, changes []bindings.SQLChange) error {
		got = changes
		return nil
	})

	payload := &pb.TypedData{Data: &pb.TypedData_Json{
		Json: `[{"Operation":0,"Item":{"ProductId":1,"Name":"Widget","Cost":100}}]`,
	}}

	invResp := runSQLInvocation(t, payload, handler)
	if invResp.Result.Status != pb.StatusResult_Success {
		t.Fatalf("expected Success, got %v (exception=%q)",
			invResp.Result.Status, invResp.Result.Exception.GetMessage())
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 change delivered to handler, got %d", len(got))
	}
	if got[0].Operation != bindings.SQLOperationInsert {
		t.Errorf("expected Insert, got %v", got[0].Operation)
	}
	var p struct {
		ProductID int    `json:"ProductId"`
		Name      string `json:"Name"`
		Cost      int    `json:"Cost"`
	}
	if err := json.Unmarshal(got[0].Item, &p); err != nil {
		t.Fatalf("failed to decode Item: %v", err)
	}
	if p.ProductID != 1 || p.Name != "Widget" || p.Cost != 100 {
		t.Errorf("unexpected decoded item: %+v", p)
	}
}

func TestHandleInvocationRequest_SQLChanges_Batch(t *testing.T) {
	var got []bindings.SQLChange
	var calls int32
	handler := sdk.SQLChangeHandler(func(ctx context.Context, changes []bindings.SQLChange) error {
		atomic.AddInt32(&calls, 1)
		got = changes
		return nil
	})

	payload := &pb.TypedData{Data: &pb.TypedData_Json{Json: changesPayload}}

	invResp := runSQLInvocation(t, payload, handler)
	if invResp.Result.Status != pb.StatusResult_Success {
		t.Fatalf("expected Success, got %v (exception=%q)",
			invResp.Result.Status, invResp.Result.Exception.GetMessage())
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("expected handler invoked exactly once, got %d", calls)
	}
	if len(got) != 3 {
		t.Fatalf("expected batch of 3 changes, got %d", len(got))
	}
	wantOps := []bindings.SQLOperation{
		bindings.SQLOperationInsert,
		bindings.SQLOperationUpdate,
		bindings.SQLOperationDelete,
	}
	for i, want := range wantOps {
		if got[i].Operation != want {
			t.Errorf("change[%d]: expected %v, got %v", i, want, got[i].Operation)
		}
	}
}

func TestHandleInvocationRequest_SQLChanges_HandlerError(t *testing.T) {
	wantErr := errors.New("handler failed")
	handler := sdk.SQLChangeHandler(func(ctx context.Context, changes []bindings.SQLChange) error {
		return wantErr
	})

	payload := &pb.TypedData{Data: &pb.TypedData_Json{Json: changesPayload}}

	invResp := runSQLInvocation(t, payload, handler)
	if invResp.Result.Status != pb.StatusResult_Failure {
		t.Fatalf("expected Failure, got %v", invResp.Result.Status)
	}
	if invResp.Result.Exception == nil {
		t.Fatal("expected RpcException populated from handler error; got nil")
	}
	if invResp.Result.Exception.Message != wantErr.Error() {
		t.Errorf("expected exception message %q, got %q",
			wantErr.Error(), invResp.Result.Exception.Message)
	}
}

// TestHandleInvocationRequest_SQLChanges_MalformedItem confirms that bad
// JSON inside an Item field does NOT crash the worker — the SQLChange wrapper
// still decodes (Item is json.RawMessage), and the user's handler is
// responsible for per-row decoding. The host successfully reports invocation
// completion regardless of what's inside Item.
func TestHandleInvocationRequest_SQLChanges_MalformedItem(t *testing.T) {
	var got []bindings.SQLChange
	handler := sdk.SQLChangeHandler(func(ctx context.Context, changes []bindings.SQLChange) error {
		got = changes
		return nil
	})

	// Item is structurally valid JSON but won't unmarshal into a Product
	// struct (wrong field types).
	payload := &pb.TypedData{Data: &pb.TypedData_Json{
		Json: `[{"Operation":0,"Item":{"ProductId":"not-an-int","Name":42}}]`,
	}}

	invResp := runSQLInvocation(t, payload, handler)
	if invResp.Result.Status != pb.StatusResult_Success {
		t.Fatalf("expected Success (Item is opaque to worker), got %v (exception=%q)",
			invResp.Result.Status, invResp.Result.Exception.GetMessage())
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 change, got %d", len(got))
	}
	// Item must be passed through unchanged for the handler to inspect.
	if len(got[0].Item) == 0 {
		t.Errorf("expected Item to be passed through, got empty bytes")
	}
}

// TestHandleInvocationRequest_SQLChanges_FromString covers the alternative
// wire format where the host delivers the batch as TypedData_String_ rather
// than TypedData_Json. Older host versions have been observed to use this.
func TestHandleInvocationRequest_SQLChanges_FromString(t *testing.T) {
	var got []bindings.SQLChange
	handler := sdk.SQLChangeHandler(func(ctx context.Context, changes []bindings.SQLChange) error {
		got = changes
		return nil
	})

	payload := &pb.TypedData{Data: &pb.TypedData_String_{String_: changesPayload}}

	invResp := runSQLInvocation(t, payload, handler)
	if invResp.Result.Status != pb.StatusResult_Success {
		t.Fatalf("expected Success, got %v (exception=%q)",
			invResp.Result.Status, invResp.Result.Exception.GetMessage())
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 changes from String_ payload, got %d", len(got))
	}
	if got[0].Operation != bindings.SQLOperationInsert {
		t.Errorf("change[0]: expected Insert, got %v", got[0].Operation)
	}
	if got[2].Operation != bindings.SQLOperationDelete {
		t.Errorf("change[2]: expected Delete, got %v", got[2].Operation)
	}
}

// TestHandleInvocationRequest_SQLChanges_MalformedOuterJSON confirms that
// a truncated/malformed outer JSON array surfaces as a Failure status with
// a descriptive exception, not a silent empty slice or a worker crash.
func TestHandleInvocationRequest_SQLChanges_MalformedOuterJSON(t *testing.T) {
	handler := sdk.SQLChangeHandler(func(ctx context.Context, changes []bindings.SQLChange) error {
		t.Error("handler should not be invoked on decode failure")
		return nil
	})

	app := newTestApp()
	disp := newTestDispatcher("req-sql")
	disp.App = app

	app.SQL("productsChanged", handler,
		sdk.WithTable("dbo.Products"),
		sdk.WithConnection("AzureWebJobsSqlConnectionString"),
	)

	var funcID string
	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		funcID = key.(string)
		return false
	})

	handleFunctionLoadRequest(&pb.FunctionLoadRequest{FunctionId: funcID}, disp, "req-sql")

	resp, err := handleInvocationRequest(&pb.InvocationRequest{
		FunctionId:   funcID,
		InvocationId: "inv-1",
		InputData: []*pb.ParameterBinding{
			{
				Name: "changes",
				RpcData: &pb.ParameterBinding_Data{
					Data: &pb.TypedData{Data: &pb.TypedData_Json{
						Json: `[{"Operation":0,"Item":{"ProductId":1}`, // truncated
					}},
				},
			},
		},
	}, disp, "req-sql")

	// The worker may surface decode errors either as an error return or as a
	// Failure InvocationResponse — both are acceptable as long as the handler
	// is never invoked with corrupt data.
	if err != nil {
		// Decode error surfaced before response construction — acceptable.
		return
	}

	invResp := resp.GetContent().(*pb.StreamingMessage_InvocationResponse).InvocationResponse
	if invResp.Result.Status != pb.StatusResult_Failure {
		t.Fatalf("expected Failure for malformed outer JSON, got %v", invResp.Result.Status)
	}
}
