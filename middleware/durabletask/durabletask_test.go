package durabletask

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/protobuf/proto"
)

// --- Sample orchestration used by the tests (mirrors samples/durableFunctions) ---

// helloCities calls the SayHello activity once per city, in sequence.
func helloCities(ctx *task.OrchestrationContext) (any, error) {
	var result []string
	for _, city := range []string{"Tokyo", "Seattle", "London"} {
		var greeting string
		if err := ctx.CallActivity("SayHello", task.WithActivityInput(city)).Await(&greeting); err != nil {
			return nil, err
		}
		result = append(result, greeting)
	}
	return result, nil
}

// sayHello is the activity in the worker's plain-function form.
func sayHello(_ context.Context, city string) (string, error) {
	return "Hello, " + city + "!", nil
}

// newEmulatorBackend stands up an in-memory durabletask backend (the
// "emulator") and returns it plus a client.
func newEmulatorBackend(t *testing.T) (backend.Backend, backend.TaskHubClient) {
	t.Helper()
	ctx := context.Background()
	logger := backend.DefaultLogger()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	if err := be.CreateTaskHub(ctx); err != nil {
		t.Fatalf("create task hub: %v", err)
	}
	if err := be.Start(ctx); err != nil {
		t.Fatalf("start backend: %v", err)
	}
	t.Cleanup(func() { _ = be.Stop(ctx) })
	return be, backend.NewTaskHubClient(be)
}

// scheduleAndEncodeRequest schedules a new orchestration on the emulator and
// returns the first work item rendered as a base64 OrchestratorRequest — the
// exact payload the Functions host would send to the worker for the first
// orchestrator turn.
func scheduleAndEncodeRequest(t *testing.T, be backend.Backend, client backend.TaskHubClient, name string, input any) string {
	t.Helper()
	ctx := context.Background()
	if _, err := client.ScheduleNewOrchestration(ctx, name, api.WithInput(input)); err != nil {
		t.Fatalf("schedule orchestration: %v", err)
	}
	wi, err := be.GetOrchestrationWorkItem(ctx)
	if err != nil {
		t.Fatalf("get work item: %v", err)
	}
	reqBytes, err := encodeOrchestratorRequest(string(wi.InstanceID), nil, wi.NewEvents)
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	return base64.StdEncoding.EncodeToString(reqBytes)
}

// TestOrchestratorReplay_MatchesEngine drives a real (emulator-produced)
// OrchestratorRequest through the middleware's runner and asserts that the
// resulting OrchestratorResponse (a) schedules the first activity and (b) is
// byte-for-byte equivalent to what durabletask-go's executor produces
// directly. This exercises the full decode -> replay -> encode path.
func TestOrchestratorReplay_MatchesEngine(t *testing.T) {
	ctx := context.Background()
	dt := Middleware(
		WithOrchestrator("HelloCities", helloCities),
		WithActivity("SayHello", sayHello),
	)

	be, client := newEmulatorBackend(t)
	encoded := scheduleAndEncodeRequest(t, be, client, "HelloCities", "")

	gotB64, err := dt.runner.loadAndRun(ctx, encoded)
	if err != nil {
		t.Fatalf("loadAndRun: %v", err)
	}
	gotBytes, err := base64.StdEncoding.DecodeString(gotB64)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}

	// The first orchestrator action schedules the SayHello activity; its
	// name is embedded as a string in the ScheduleTaskAction.
	if !strings.Contains(string(gotBytes), "SayHello") {
		t.Fatalf("expected response to schedule SayHello, got %q", string(gotBytes))
	}

	// Equivalence with the engine's direct output.
	reqBytes, _ := base64.StdEncoding.DecodeString(encoded)
	id, past, newEvents, err := decodeOrchestratorRequest(reqBytes)
	if err != nil {
		t.Fatalf("decode request: %v", err)
	}
	executor := task.NewTaskExecutor(dt.registry)
	direct, err := executor.ExecuteOrchestrator(ctx, api.InstanceID(id), past, newEvents)
	if err != nil {
		t.Fatalf("direct execute: %v", err)
	}
	fresh := direct.Response.ProtoReflect().New().Interface()
	if err := proto.Unmarshal(gotBytes, fresh); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if !proto.Equal(direct.Response, fresh) {
		t.Fatalf("middleware response does not match engine response")
	}
}

// TestMiddlewareIntegration_OrchestrationShortCircuits exercises the real SDK
// seams: App.Use registers the provided functions and the composed middleware
// chain short-circuits orchestration invocations, producing the response via
// mc.SetReturnValue without invoking the inner handler.
func TestMiddlewareIntegration_OrchestrationShortCircuits(t *testing.T) {
	ctx := context.Background()
	dt := Middleware(
		WithOrchestrator("HelloCities", helloCities),
		WithActivity("SayHello", sayHello),
	)

	app := sdk.FunctionApp()
	app.Use(dt)

	names := registeredFunctionNames(app)
	if !names["HelloCities"] {
		t.Fatalf("orchestrator HelloCities was not registered on the app: %v", names)
	}
	if !names["SayHello"] {
		t.Fatalf("activity SayHello was not registered on the app: %v", names)
	}

	be, client := newEmulatorBackend(t)
	encoded := scheduleAndEncodeRequest(t, be, client, "HelloCities", "")

	innerCalled := false
	inner := func(_ context.Context, _ *sdk.MiddlewareContext) error {
		innerCalled = true
		return nil
	}
	chain := app.Compose(inner)

	mc := &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{
		FunctionName: "HelloCities",
		TriggerType:  string(OrchestrationTriggerType),
	}}
	mc.SetInputBytes([]byte(encoded))

	if err := chain(ctx, mc); err != nil {
		t.Fatalf("chain: %v", err)
	}
	if innerCalled {
		t.Fatal("orchestration invocation should short-circuit, not call inner")
	}

	got, ok := mc.ReturnValue()
	if !ok {
		t.Fatal("expected a return value to be set")
	}
	gotStr, ok := got.(string)
	if !ok {
		t.Fatalf("expected return value to be a base64 string, got %T", got)
	}
	gotBytes, err := base64.StdEncoding.DecodeString(gotStr)
	if err != nil {
		t.Fatalf("decode return value: %v", err)
	}
	if !strings.Contains(string(gotBytes), "SayHello") {
		t.Fatalf("expected orchestrator response to schedule SayHello")
	}
}

// TestMiddlewareIntegration_ActivityPassesThrough verifies that
// activity (and other non-orchestration) invocations flow through to the
// normal pipeline rather than being short-circuited, and that a configured
// durable client is attached to the invocation context for starters.
func TestMiddlewareIntegration_ActivityPassesThrough(t *testing.T) {
	// A configured client (here over the in-memory sidecar) is attached to
	// non-orchestration invocations so HTTP starters can reach it.
	conn := startGrpcSidecar(t, task.NewTaskRegistry())
	dt := Middleware(WithActivity("SayHello", sayHello), WithClient(NewClient(conn)))
	app := sdk.FunctionApp()
	app.Use(dt)

	innerCalled := false
	var clientPresent bool
	inner := func(ctx context.Context, _ *sdk.MiddlewareContext) error {
		innerCalled = true
		_, clientPresent = ClientFromContext(ctx)
		return nil
	}
	chain := app.Compose(inner)

	mc := &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{
		FunctionName: "SayHello",
		TriggerType:  string(ActivityTriggerType),
	}}

	if err := chain(context.Background(), mc); err != nil {
		t.Fatalf("chain: %v", err)
	}
	if !innerCalled {
		t.Fatal("activity invocation should pass through to inner")
	}
	if !clientPresent {
		t.Fatal("durable client should be attached to non-orchestration context")
	}
}

// TestEndToEnd_Emulator runs the full orchestration to completion on the
// in-memory durabletask worker, validating that the orchestrator logic in the
// sample produces the expected output across multiple replay turns.
func TestEndToEnd_Emulator(t *testing.T) {
	ctx := context.Background()
	r := task.NewTaskRegistry()
	if err := r.AddOrchestratorN("HelloCities", helloCities); err != nil {
		t.Fatalf("add orchestrator: %v", err)
	}
	if err := r.AddActivityN("SayHello", func(actx task.ActivityContext) (any, error) {
		var city string
		if err := actx.GetInput(&city); err != nil {
			return nil, err
		}
		return "Hello, " + city + "!", nil
	}); err != nil {
		t.Fatalf("add activity: %v", err)
	}

	logger := backend.DefaultLogger()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	executor := task.NewTaskExecutor(r)
	orchestrationWorker := backend.NewOrchestrationWorker(be, executor, logger)
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
	hub := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger)
	if err := hub.Start(ctx); err != nil {
		t.Fatalf("start hub: %v", err)
	}
	defer func() { _ = hub.Shutdown(ctx) }()

	client := backend.NewTaskHubClient(be)
	id, err := client.ScheduleNewOrchestration(ctx, "HelloCities", api.WithInput(""))
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	if err != nil {
		t.Fatalf("wait for completion: %v", err)
	}
	if metadata.RuntimeStatus != api.RUNTIME_STATUS_COMPLETED {
		t.Fatalf("expected COMPLETED, got %v", metadata.RuntimeStatus)
	}
	for _, want := range []string{"Hello, Tokyo!", "Hello, Seattle!", "Hello, London!"} {
		if !strings.Contains(metadata.SerializedOutput, want) {
			t.Fatalf("output %q missing %q", metadata.SerializedOutput, want)
		}
	}
}

func registeredFunctionNames(app *sdk.App) map[string]bool {
	names := map[string]bool{}
	app.GetRegisteredFunctions().Range(func(_, v any) bool {
		names[v.(*sdk.RegisteredFunction).FuncName] = true
		return true
	})
	return names
}
