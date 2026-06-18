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
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
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

// TestLoadAndRun_AnnotatesTurnSpan verifies that when an active (recording)
// span is on the context — as the otelfunc middleware arranges — the runner
// decorates it with per-turn Durable diagnostics. The first turn has no past
// events (not a replay) and schedules the first activity (>=1 action).
func TestLoadAndRun_AnnotatesTurnSpan(t *testing.T) {
	dt := Middleware(
		WithOrchestrator("HelloCities", helloCities),
		WithActivity("SayHello", sayHello),
	)

	be, client := newEmulatorBackend(t)
	encoded := scheduleAndEncodeRequest(t, be, client, "HelloCities", "")

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	ctx, span := tp.Tracer("test").Start(context.Background(), "function:HelloCities")

	if _, err := dt.runner.loadAndRun(ctx, encoded); err != nil {
		t.Fatalf("loadAndRun: %v", err)
	}
	span.End()

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 recorded span, got %d", len(spans))
	}
	attrs := spans[0].Attributes

	if v, ok := attrValue(attrs, "durabletask.is_replay"); !ok || v.AsBool() {
		t.Errorf("durabletask.is_replay = %v (present=%v), want false", v.AsBool(), ok)
	}
	if v, ok := attrValue(attrs, "durabletask.history_event_count"); !ok || v.AsInt64() != 0 {
		t.Errorf("durabletask.history_event_count = %d (present=%v), want 0", v.AsInt64(), ok)
	}
	if v, ok := attrValue(attrs, "durabletask.new_events_count"); !ok || v.AsInt64() < 1 {
		t.Errorf("durabletask.new_events_count = %d (present=%v), want >= 1", v.AsInt64(), ok)
	}
	if v, ok := attrValue(attrs, "durabletask.action_count"); !ok || v.AsInt64() < 1 {
		t.Errorf("durabletask.action_count = %d (present=%v), want >= 1", v.AsInt64(), ok)
	}
	if v, ok := attrValue(attrs, "durabletask.task.instance_id"); !ok || v.AsString() == "" {
		t.Error("durabletask.task.instance_id not set")
	}
}

// TestAnnotateTurnSpan_NoActiveSpanIsSafe documents the contract that matters
// when an app does NOT register the otelfunc middleware: there is no span on
// the context, trace.SpanFromContext returns a non-recording no-op span, and
// the annotation degrades to a harmless no-op (no panic, nothing recorded).
func TestAnnotateTurnSpan_NoActiveSpanIsSafe(t *testing.T) {
	// Must not panic with a span-less context and nil results.
	annotateTurnSpan(context.Background(), "inst-1", nil, nil, nil)
}

// attrValue returns the value of the first attribute matching key.
func attrValue(attrs []attribute.KeyValue, key string) (attribute.Value, bool) {
	for _, kv := range attrs {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
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

// TestActivityInputDecoder verifies the wrapper JSON-decodes activity inputs of
// various shapes (scalar, struct, none) and forwards outputs unchanged, so a
// func(ctx, string) activity receives Tokyo rather than "Tokyo".
func TestActivityInputDecoder(t *testing.T) {
	// Scalar string input: the JSON "Tokyo" must arrive as Tokyo (no quotes).
	stringAct := activityInputDecoder(func(_ context.Context, city string) (string, error) {
		return "Hello, " + city + "!", nil
	})
	if out, err := stringAct(context.Background(), []byte(`"Tokyo"`)); err != nil {
		t.Fatalf("string activity: %v", err)
	} else if out != "Hello, Tokyo!" {
		t.Errorf("string activity out = %q, want Hello, Tokyo!", out)
	}

	// Struct input.
	type expense struct {
		ID     string  `json:"id"`
		Amount float64 `json:"amount"`
	}
	structAct := activityInputDecoder(func(_ context.Context, e expense) (float64, error) {
		return e.Amount, nil
	})
	if out, err := structAct(context.Background(), []byte(`{"id":"x","amount":42.5}`)); err != nil {
		t.Fatalf("struct activity: %v", err)
	} else if out != 42.5 {
		t.Errorf("struct activity out = %v, want 42.5", out)
	}

	// No input.
	noInputAct := activityInputDecoder(func(_ context.Context) (string, error) {
		return "ok", nil
	})
	if out, err := noInputAct(context.Background(), nil); err != nil {
		t.Fatalf("no-input activity: %v", err)
	} else if out != "ok" {
		t.Errorf("no-input activity out = %q, want ok", out)
	}

	// No output (error-only signature) with a scalar input.
	var ran bool
	errOnlyAct := activityInputDecoder(func(_ context.Context, n int) error {
		ran = n == 7
		return nil
	})
	if out, err := errOnlyAct(context.Background(), []byte(`7`)); err != nil {
		t.Fatalf("error-only activity: %v", err)
	} else if out != nil {
		t.Errorf("error-only activity out = %v, want nil", out)
	}
	if !ran {
		t.Error("error-only activity did not receive decoded input 7")
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
