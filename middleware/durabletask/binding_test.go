package durabletask

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/microsoft/durabletask-go/api"
	"google.golang.org/grpc/connectivity"
)

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

func TestEntityTriggerBinding(t *testing.T) {
	binding := entityTriggerBinding{}.ToBinding()
	if binding.Type != string(EntityTriggerType) {
		t.Fatalf("entity trigger type = %q, want %q", binding.Type, EntityTriggerType)
	}
	if binding.Name != entityParamName {
		t.Fatalf("entity trigger parameter = %q, want %q", binding.Name, entityParamName)
	}
	if binding.Direction != "in" {
		t.Fatalf("entity trigger direction = %q, want in", binding.Direction)
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

// The binding chooses the RPC endpoint, reuses its client, and gives SDK
// shutdown ownership of the connection. The RPC reply is fixed, not execution.
func TestDurable_ClientBinding_ProtocolAndLifetime(t *testing.T) {
	// Ensure the env fallback is inactive so we exercise only the binding path.
	t.Setenv(EnvGrpcEndpoint, "")

	addr := startManagementStub(t, "StartInstance", map[string]any{
		"name": "Hello", "instanceId": "binding-instance", "input": `"World"`,
	}, `{"instanceId":"binding-instance"}`)

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
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := client.ScheduleNewOrchestration(ctx, "Hello",
		api.WithInstanceID("binding-instance"), api.WithInput("World"))
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if id != "binding-instance" {
		t.Fatalf("instance ID = %q, want fixed binding-instance reply", id)
	}

	// The per-endpoint client is cached and reused across invocations.
	if again := runStarter(); again != client {
		t.Error("expected the binding client to be reused across invocations")
	}

	// Installing the extension retains SDK-owned shutdown of the cached client.
	app := sdk.FunctionApp()
	app.Use(d)
	if client.conn.GetState() == connectivity.Shutdown {
		t.Fatal("connection was already shut down before SDK shutdown")
	}
	if err := app.RunShutdowns(context.Background()); err != nil {
		t.Errorf("shutdown: %v", err)
	}
	if state := client.conn.GetState(); state != connectivity.Shutdown {
		t.Errorf("connection state after SDK shutdown = %v, want Shutdown", state)
	}
}
