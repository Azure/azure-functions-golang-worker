package durabletask

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/grpc"
)

// startGrpcSidecarTCP is the TCP-listener variant of startGrpcSidecar. It
// returns the "host:port" address of the durabletask gRPC sidecar so a test
// can exercise the real dial path (grpc.NewClient against an address), which
// is what the durable client binding delivery uses — unlike the bufconn
// helper, which bypasses the address with a custom dialer.
func startGrpcSidecarTCP(t *testing.T, r *task.TaskRegistry) string {
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

	_, registerFn := backend.NewGrpcExecutor(be, logger)
	grpcServer := grpc.NewServer()
	registerFn(grpcServer)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)
	return lis.Addr().String()
}

// TestGrpcTarget verifies the rpcBaseUrl -> gRPC dial target conversion the
// durable client binding relies on.
func TestGrpcTarget(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"http://127.0.0.1:54321/", "127.0.0.1:54321"},
		{"http://127.0.0.1:54321", "127.0.0.1:54321"},
		{"https://example.com:443/durabletask", "example.com:443"},
		{"127.0.0.1:4001", "127.0.0.1:4001"}, // already a bare host:port
	}
	for _, c := range cases {
		if got := grpcTarget(c.in); got != c.want {
			t.Errorf("grpcTarget(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestClientInput_AddsBinding verifies the registration option appends a
// durableClient input binding so the host knows to deliver the endpoint.
func TestClientInput_AddsBinding(t *testing.T) {
	rf := &sdk.RegisteredFunction{}
	ClientInput()(rf)

	if len(rf.RawBindings) != 1 {
		t.Fatalf("RawBindings length = %d, want 1", len(rf.RawBindings))
	}
	b := rf.RawBindings[0]
	if b.Type != string(DurableClientBindingType) {
		t.Errorf("binding type = %q, want %q", b.Type, DurableClientBindingType)
	}
	if b.Direction != "in" {
		t.Errorf("binding direction = %q, want in", b.Direction)
	}
	if b.Name != durableClientParamName {
		t.Errorf("binding name = %q, want %q", b.Name, durableClientParamName)
	}
}

// TestClientFromBinding_NilCases verifies the binding-derived client is absent
// when no usable durable client binding payload is present.
func TestClientFromBinding_NilCases(t *testing.T) {
	d := Middleware()

	cases := []struct {
		name string
		mc   *sdk.MiddlewareContext
	}{
		{"nil mc", nil},
		{"no binding", &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{}}},
		{"bad json", mcWithBinding(`not json`)},
		{"empty rpcBaseUrl", mcWithBinding(`{"rpcBaseUrl":""}`)},
		{"missing rpcBaseUrl", mcWithBinding(`{"taskHubName":"default"}`)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := d.clientFromBinding(c.mc); got != nil {
				t.Errorf("clientFromBinding = %v, want nil", got)
			}
		})
	}
}

func mcWithBinding(json string) *sdk.MiddlewareContext {
	mc := &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{}}
	mc.SetBindingInput(durableClientParamName, []byte(json))
	return mc
}

// TestDurable_ClientBinding_EndToEnd proves the durable client endpoint is
// taken from the host-delivered durable client binding: a starter invocation
// carries the binding (rpcBaseUrl pointing at a real durabletask sidecar), the
// middleware connects to it, and the client retrieved via ClientFromContext
// schedules an orchestration that runs to completion. It also verifies the
// per-endpoint client is reused across invocations and closed on Shutdown.
func TestDurable_ClientBinding_EndToEnd(t *testing.T) {
	// Ensure the env fallback is inactive so we exercise only the binding path.
	t.Setenv(EnvGrpcEndpoint, "")

	r := task.NewTaskRegistry()
	if err := r.AddOrchestratorN("Hello", func(octx *task.OrchestrationContext) (any, error) {
		var greeting string
		if err := octx.CallActivity("Say", task.WithActivityInput("World")).Await(&greeting); err != nil {
			return nil, err
		}
		return greeting, nil
	}); err != nil {
		t.Fatalf("add orchestrator: %v", err)
	}
	if err := r.AddActivityN("Say", func(actx task.ActivityContext) (any, error) {
		var name string
		if err := actx.GetInput(&name); err != nil {
			return nil, err
		}
		return "Hello, " + name + "!", nil
	}); err != nil {
		t.Fatalf("add activity: %v", err)
	}

	addr := startGrpcSidecarTCP(t, r)

	d := Middleware() // no WithClient, no env endpoint
	if d.client != nil {
		t.Fatal("expected no env/explicit client")
	}

	bindingJSON := fmt.Sprintf(`{"rpcBaseUrl":"http://%s/","taskHubName":"default"}`, addr)

	// Run a non-orchestration (starter) invocation through Wrap and capture the
	// client the middleware attaches from the binding.
	runStarter := func() *Client {
		mc := &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{TriggerType: "httpTrigger"}}
		mc.SetBindingInput(durableClientParamName, []byte(bindingJSON))
		var got *Client
		chain := d.Wrap(func(ctx context.Context, _ *sdk.MiddlewareContext) error {
			got, _ = ClientFromContext(ctx)
			return nil
		})
		if err := chain(context.Background(), mc); err != nil {
			t.Fatalf("wrap: %v", err)
		}
		return got
	}

	client := runStarter()
	if client == nil {
		t.Fatal("expected a client from the durable client binding")
	}

	// The binding-derived client actually works against the sidecar.
	ctx := context.Background()
	id, err := client.ScheduleNewOrchestration(ctx, "Hello")
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	final, err := client.WaitForOrchestrationCompletion(waitCtx, id)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got := RuntimeStatus(final); got != "Completed" {
		t.Fatalf("runtime status = %q, want Completed", got)
	}

	// The per-endpoint client is cached and reused across invocations.
	if again := runStarter(); again != client {
		t.Error("expected the binding client to be reused across invocations")
	}

	// Shutdown closes the cached binding client.
	if err := d.Shutdown(context.Background()); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}
