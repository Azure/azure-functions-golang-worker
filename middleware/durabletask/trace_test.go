package durabletask

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/microsoft/durabletask-go/task"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// seenWorkItems records the per-work-item [sdk.MiddlewareContext] a middleware
// observes when the durable listener runs work items through the App chain.
type seenWorkItems struct {
	mu sync.Mutex
	// byName maps function name -> the most recent observation for it.
	byName map[string]observedWorkItem
}

type observedWorkItem struct {
	trigger     string
	traceParent string
}

func (s *seenWorkItems) record(mc *sdk.MiddlewareContext) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.byName[mc.FunctionName]
	cur.trigger = mc.TriggerType
	// Keep the first non-empty traceparent we observe for a name (replay can
	// produce later turns whose seeded context differs).
	if cur.traceParent == "" {
		cur.traceParent = mc.TraceContext.TraceParent
	}
	s.byName[mc.FunctionName] = cur
}

func (s *seenWorkItems) get(name string) (observedWorkItem, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.byName[name]
	return v, ok
}

// TestDurable_WorkItemsRunThroughChain proves the model-2 tracing seam end to
// end on the worker side: registering [Durable] via App.Use injects the chain
// composer, so every orchestration and activity work item runs through the
// App's middleware chain. A capturing middleware (standing in for otelfunc)
// observes the per-work-item [sdk.MiddlewareContext] the listener synthesizes,
// confirming the function name, durable trigger type, and — for the activity —
// the parent W3C trace context propagated from the orchestration (the
// ActivityRequest.ParentTraceContext the backend stamps when a sampled span is
// active).
func TestDurable_WorkItemsRunThroughChain(t *testing.T) {
	// A sampled global provider makes the in-process backend emit a sampled
	// orchestration span, which it stamps onto scheduled activities. Without
	// sampling there is no trace context to propagate.
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	conn := startListenerSidecar(t)

	seen := &seenWorkItems{byName: map[string]observedWorkItem{}}
	capture := sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
			seen.record(mc)
			return next(ctx, mc)
		}
	})

	dt := Middleware(
		WithConnection(conn),
		WithOrchestrator("HelloChain", func(octx *task.OrchestrationContext) (any, error) {
			var r string
			if err := octx.CallActivity("SayChain", task.WithActivityInput("Tokyo")).Await(&r); err != nil {
				return nil, err
			}
			return r, nil
		}),
		WithActivity("SayChain", func(actx task.ActivityContext) (any, error) {
			var city string
			if err := actx.GetInput(&city); err != nil {
				return nil, err
			}
			return "Hello, " + city + "!", nil
		}),
	)

	// Registering through App.Use is what injects the composer (sdk.ComposerAware).
	app := sdk.FunctionApp()
	app.Use(capture)
	app.Use(dt)

	startHook(t, dt)

	client := dt.Client()
	if client == nil {
		t.Fatal("expected a client after Start")
	}

	ctx := context.Background()
	id, err := client.ScheduleNewOrchestration(ctx, "HelloChain", nil)
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

	// The orchestration ran through the chain as an orchestrationTrigger.
	orch, ok := seen.get("HelloChain")
	if !ok {
		t.Fatal("orchestration work item never ran through the middleware chain")
	}
	if orch.trigger != "orchestrationTrigger" {
		t.Errorf("orchestration trigger = %q, want orchestrationTrigger", orch.trigger)
	}

	// The activity ran through the chain as an activityTrigger and carried the
	// orchestration's parent trace context (proves end-to-end propagation:
	// backend -> ActivityRequest.ParentTraceContext -> WorkItemInfo ->
	// MiddlewareContext).
	act, ok := seen.get("SayChain")
	if !ok {
		t.Fatal("activity work item never ran through the middleware chain")
	}
	if act.trigger != "activityTrigger" {
		t.Errorf("activity trigger = %q, want activityTrigger", act.trigger)
	}
	if act.traceParent == "" {
		t.Error("activity work item carried no parent trace context; propagation broken")
	}
}
