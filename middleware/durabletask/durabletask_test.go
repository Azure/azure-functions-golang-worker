package durabletask

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// startListenerSidecar stands up a durabletask-go gRPC sidecar whose executor
// DISPATCHES work items over the gRPC stream (backend.NewGrpcExecutor) instead
// of executing in-process. This is the model-2 topology: the host sidecar
// dispatches; a connected work-item listener (our [Durable]) executes the
// work. Returns a client connection to the sidecar.
func startListenerSidecar(t *testing.T) *grpc.ClientConn {
	t.Helper()
	ctx := context.Background()
	logger := backend.DefaultLogger()

	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	grpcExecutor, registerFn := backend.NewGrpcExecutor(be, logger)
	orchestrationWorker := backend.NewOrchestrationWorker(be, grpcExecutor, logger)
	activityWorker := backend.NewActivityTaskWorker(be, grpcExecutor, logger)
	hub := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger)
	if err := hub.Start(ctx); err != nil {
		t.Fatalf("start hub: %v", err)
	}
	t.Cleanup(func() { _ = hub.Shutdown(context.Background()) })

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

// startHook starts dt's lifecycle hook (the work-item listener) and registers
// its shutdown — the same hook App.Use contributes and the worker runs.
func startHook(t *testing.T, dt *Durable) {
	t.Helper()
	h := dt.Lifecycle()
	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = h.Shutdown(context.Background()) })
}

// TestDurable_EndToEnd_Listener exercises the full model-2 path: the lifecycle
// hook starts a work-item listener against a sidecar that dispatches over the
// gRPC stream, the management client schedules an orchestration, the listener
// executes the orchestrator + activities, and the client observes completion.
func TestDurable_EndToEnd_Listener(t *testing.T) {
	conn := startListenerSidecar(t)

	dt := Middleware(
		WithConnection(conn),
		WithOrchestrator("HelloCities", func(octx *task.OrchestrationContext) (any, error) {
			out := make([]string, 0, 3)
			for _, city := range []string{"Tokyo", "Seattle", "London"} {
				var r string
				if err := octx.CallActivity("SayHello", task.WithActivityInput(city)).Await(&r); err != nil {
					return nil, err
				}
				out = append(out, r)
			}
			return out, nil
		}),
		WithActivity("SayHello", func(actx task.ActivityContext) (any, error) {
			var city string
			if err := actx.GetInput(&city); err != nil {
				return nil, err
			}
			return "Hello, " + city + "!", nil
		}),
	)

	startHook(t, dt)

	client := dt.Client()
	if client == nil {
		t.Fatal("expected a client after Start")
	}

	ctx := context.Background()
	id, err := client.ScheduleNewOrchestration(ctx, "HelloCities", nil)
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	final, err := client.WaitForCompletion(waitCtx, id)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if final.RuntimeStatus != "Completed" {
		t.Fatalf("status = %q, want Completed", final.RuntimeStatus)
	}
	for _, want := range []string{"Tokyo", "Seattle", "London"} {
		if !strings.Contains(final.Output, want) {
			t.Fatalf("output %q missing %q", final.Output, want)
		}
	}
}

// TestDurable_EndToEnd_HITL drives an external-event wait and delivers the
// approval via the client — the human-in-the-loop pattern — all executed by
// the Durable listener.
func TestDurable_EndToEnd_HITL(t *testing.T) {
	conn := startListenerSidecar(t)

	dt := Middleware(
		WithConnection(conn),
		WithOrchestrator("Approval", func(octx *task.OrchestrationContext) (any, error) {
			octx.SetCustomStatus("awaiting approval")
			var d struct {
				Approved bool `json:"approved"`
			}
			if err := octx.WaitForSingleEvent("ApprovalDecision", 30*time.Second).Await(&d); err != nil {
				return "timeout", nil
			}
			if d.Approved {
				return "approved", nil
			}
			return "rejected", nil
		}),
	)
	startHook(t, dt)

	ctx := context.Background()
	client := dt.Client()
	id, err := client.ScheduleNewOrchestration(ctx, "Approval", nil)
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}

	if !waitForCustomStatus(t, client, id, "awaiting approval", 10*time.Second) {
		st, _ := client.GetStatus(ctx, id)
		t.Fatalf("never reached approval wait; last = %+v", st)
	}
	if err := client.RaiseEvent(ctx, id, "ApprovalDecision", map[string]any{"approved": true}); err != nil {
		t.Fatalf("raise event: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	final, err := client.WaitForCompletion(waitCtx, id)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if !strings.Contains(final.Output, "approved") {
		t.Fatalf("output = %q, want approved", final.Output)
	}
}

// TestDurable_SelfRegisters verifies that a single App.Use(Middleware(...))
// both composes the client-injection middleware and contributes the listener
// lifecycle hook — so the user does not register the hook separately.
func TestDurable_SelfRegisters(t *testing.T) {
	conn := startListenerSidecar(t)
	dt := Middleware(WithConnection(conn))

	app := sdk.FunctionApp()
	app.Use(dt)

	// App.Use should have collected the lifecycle hook via LifecycleProvider.
	hooks := app.LifecycleHooks()
	if len(hooks) != 1 {
		t.Fatalf("expected 1 lifecycle hook from App.Use, got %d", len(hooks))
	}
	if hooks[0] != dt.Lifecycle() {
		t.Fatal("collected hook should be the Durable's listener hook")
	}

	// Start via the collected hook (as the worker would), then verify the
	// composed chain injects the client.
	if err := hooks[0].Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = hooks[0].Shutdown(context.Background()) })

	var got *Client
	var present bool
	chain := app.Compose(func(ctx context.Context, _ *sdk.MiddlewareContext) error {
		got, present = ClientFromContext(ctx)
		return nil
	})
	mc := &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{FunctionName: "start"}}
	if err := chain(context.Background(), mc); err != nil {
		t.Fatalf("chain: %v", err)
	}
	if !present || got == nil {
		t.Fatal("expected durable client injected into context")
	}
	if got != dt.Client() {
		t.Fatal("injected client should be the Durable's client")
	}
}

// TestDurable_Start_NoEndpoint verifies Start is a graceful no-op (durable
// inactive) when no endpoint or connection is configured.
func TestDurable_Start_NoEndpoint(t *testing.T) {
	t.Setenv(EnvGrpcEndpoint, "")
	dt := Middleware()
	if err := dt.Lifecycle().Start(context.Background()); err != nil {
		t.Fatalf("expected nil error when no endpoint configured, got %v", err)
	}
	if dt.Client() != nil {
		t.Fatal("expected no client when durable is inactive")
	}
}
