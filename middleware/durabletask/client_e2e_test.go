package durabletask

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// startGrpcSidecar stands up a durabletask-go gRPC "sidecar" backed by an
// in-memory sqlite store, with in-process workers that execute the registered
// orchestrations/activities. This is the same TaskHubSidecarService protocol
// the Durable Task Scheduler (DTS) speaks, so it is a faithful end-to-end
// target for the management [Client] (which wraps durabletask-go's
// TaskHubGrpcClient).
//
// Execution is in-process (task.NewTaskExecutor), so no separate gRPC work
// item listener is needed; the gRPC server is purely the management
// front-door that the Client dials.
func startGrpcSidecar(t *testing.T, r *task.TaskRegistry) *grpc.ClientConn {
	t.Helper()
	ctx := context.Background()
	logger := backend.DefaultLogger()

	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	executor := task.NewTaskExecutor(r)
	orchestrationWorker := backend.NewOrchestrationWorker(be, executor, logger)
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
	hub := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger)
	if err := hub.Start(ctx); err != nil {
		t.Fatalf("start hub: %v", err)
	}
	t.Cleanup(func() { _ = hub.Shutdown(context.Background()) })

	// gRPC management front-door over an in-memory listener.
	_, registerFn := backend.NewGrpcExecutor(be, logger)
	grpcServer := grpc.NewServer()
	registerFn(grpcServer)

	lis := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestClient_EndToEnd_GrpcSidecar exercises the management Client end to end
// against a real durabletask gRPC server: schedule → wait for the orchestrator
// to reach the HITL wait point → query status (custom status = progress) →
// raise the approval event (HITL response) → wait for completion. The
// orchestration uses fan-out/fan-in and an external event, the same patterns
// the durableFunctions sample relies on.
func TestClient_EndToEnd_GrpcSidecar(t *testing.T) {
	ctx := context.Background()

	r := task.NewTaskRegistry()
	if err := r.AddOrchestratorN("ProcessExpense", func(octx *task.OrchestrationContext) (any, error) {
		// fan-out / fan-in
		t1 := octx.CallActivity("Check", task.WithActivityInput("receipt"))
		t2 := octx.CallActivity("Check", task.WithActivityInput("policy"))
		var ok1, ok2 bool
		if err := t1.Await(&ok1); err != nil {
			return nil, err
		}
		if err := t2.Await(&ok2); err != nil {
			return nil, err
		}

		octx.SetCustomStatus("awaiting manager approval") // progress channel

		var d struct {
			Approved bool `json:"approved"`
		}
		if err := octx.WaitForSingleEvent("ApprovalDecision", 30*time.Second).Await(&d); err != nil {
			return "rejected: timed out", nil
		}
		if d.Approved && ok1 && ok2 {
			return "approved", nil
		}
		return "rejected", nil
	}); err != nil {
		t.Fatalf("add orchestrator: %v", err)
	}
	if err := r.AddActivityN("Check", func(task.ActivityContext) (any, error) {
		return true, nil
	}); err != nil {
		t.Fatalf("add activity: %v", err)
	}

	conn := startGrpcSidecar(t, r)
	client := NewClient(conn) // embeds durabletask-go's TaskHubGrpcClient

	// 1. Start via the Client.
	id, err := client.ScheduleNewOrchestration(ctx, "ProcessExpense",
		api.WithInput(map[string]any{"amount": 750}))
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if id == "" {
		t.Fatal("expected a non-empty instance id")
	}

	// 2. Poll status until the orchestrator reports it is awaiting approval.
	//    This proves custom status (progress) flows through the client.
	if !waitForCustomStatus(t, client, id, "awaiting manager approval", 10*time.Second) {
		st, _ := client.FetchOrchestrationMetadata(ctx, id)
		t.Fatalf("orchestration never reached approval wait; last status = %+v", st)
	}

	// 3. Deliver the HITL response via the Client.
	if err := client.RaiseEvent(ctx, id, "ApprovalDecision",
		api.WithEventPayload(map[string]any{"approved": true})); err != nil {
		t.Fatalf("raise event: %v", err)
	}

	// 4. Wait for completion via the Client and assert the approved output.
	final, err := client.WaitForOrchestrationCompletion(ctx, id)
	if err != nil {
		t.Fatalf("wait completion: %v", err)
	}
	if got := RuntimeStatus(final); got != "Completed" {
		t.Fatalf("runtime status = %q, want Completed", got)
	}
	if !strings.Contains(final.SerializedOutput, "approved") {
		t.Fatalf("output = %q, want approved", final.SerializedOutput)
	}
}

// A missing instance surfaces durabletask-go's own sentinel, so callers use
// errors.Is with api.ErrInstanceNotFound rather than a package-local copy.
func TestClient_FetchMetadata_NotFound(t *testing.T) {
	conn := startGrpcSidecar(t, task.NewTaskRegistry())
	client := NewClient(conn)

	_, err := client.FetchOrchestrationMetadata(context.Background(), "does-not-exist")
	if !errors.Is(err, api.ErrInstanceNotFound) {
		t.Fatalf("err = %v, want api.ErrInstanceNotFound", err)
	}
}

func waitForCustomStatus(t *testing.T, c *Client, id api.InstanceID, want string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, err := c.FetchOrchestrationMetadata(context.Background(), id)
		if err == nil && strings.Contains(st.SerializedCustomStatus, want) {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
