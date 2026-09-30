package durabletask

import (
	"context"
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/microsoft/durabletask-go/task"
)

// The worker binds encoded before calling middleware. Replay must nevertheless
// read the carrier at execution time, as Durable.Wrap did before the adapter.
func TestRegisteredAdapter_PreservesMiddlewareInput(t *testing.T) {
	for _, durableFirst := range []bool{true, false} {
		t.Run(durableOrderName(durableFirst), func(t *testing.T) {
			var received []string
			d := Middleware(WithOrchestrator("Echo", func(ctx *task.OrchestrationContext) (any, error) {
				var input string
				if err := ctx.GetInput(&input); err != nil {
					return nil, err
				}
				received = append(received, input)
				return input, nil
			}))
			be, client := newEmulatorBackend(t)
			original := scheduleAndEncodeRequest(t, be, client, "Echo", "original")
			instanceID, past, events := decodeRequest(t, original)
			for _, event := range events {
				if started := event.GetExecutionStarted(); started != nil {
					started.Input.Value = `"replacement"`
				}
			}
			raw, err := encodeOrchestratorRequest(instanceID, past, events)
			if err != nil {
				t.Fatal(err)
			}
			replacement := base64.StdEncoding.EncodeToString(raw)
			if replacement == original {
				t.Fatal("test must change the replay input")
			}
			app := sdk.FunctionApp()
			if durableFirst {
				app.Use(d)
			}
			var steps []string
			app.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
				return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
					mc.SetInputString(replacement)
					steps = append(steps, "replace")
					return next(ctx, mc)
				}
			}))
			app.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
				return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
					if mc.InputString() != replacement {
						t.Error("downstream middleware did not see replacement")
					}
					steps = append(steps, "observe")
					return next(ctx, mc)
				}
			}))
			if !durableFirst {
				app.Use(d)
			}
			adapter := d.provided[0].Func.(func(context.Context, string) (string, error))
			mc := &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{TriggerType: string(OrchestrationTriggerType)}}
			mc.SetInputString(original)
			ctx := sdk.ContextWithMiddleware(context.Background(), mc)
			if err := app.Compose(func(ctx context.Context, mc *sdk.MiddlewareContext) error {
				// Deliberately pass the stale argument, just as the worker does.
				_, err := adapter(ctx, original)
				return err
			})(ctx, mc); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(received, []string{"replacement"}) || !reflect.DeepEqual(steps, []string{"replace", "observe"}) {
				t.Fatalf("received=%v steps=%v; replay must use middleware's replacement", received, steps)
			}
		})
	}
}

func TestRegisteredAdapter_InputContext(t *testing.T) {
	var received string
	d := Middleware(WithOrchestrator("Echo", func(ctx *task.OrchestrationContext) (any, error) {
		if err := ctx.GetInput(&received); err != nil {
			return nil, err
		}
		return received, nil
	}))
	be, client := newEmulatorBackend(t)
	original := scheduleAndEncodeRequest(t, be, client, "Echo", "original")
	replacement := scheduleAndEncodeRequest(t, be, client, "Echo", "replacement")
	adapter := d.provided[0].Func.(func(context.Context, string) (string, error))
	for _, tc := range []struct {
		name    string
		derive  func(context.Context, *sdk.MiddlewareContext) context.Context
		want    string
		wantErr string
	}{
		{"preserved", func(ctx context.Context, _ *sdk.MiddlewareContext) context.Context { return ctx }, "replacement", ""},
		{"without cancellation", func(ctx context.Context, _ *sdk.MiddlewareContext) context.Context {
			return context.WithoutCancel(ctx)
		}, "replacement", ""},
		{"reattached to fresh context", func(_ context.Context, mc *sdk.MiddlewareContext) context.Context {
			return sdk.ContextWithMiddleware(context.Background(), mc)
		}, "replacement", ""},
		// Discarding or shadowing the carrier is not input preservation. Pin the
		// documented boundary rather than silently promising general rebinding.
		{"discarded carrier uses bound input", func(context.Context, *sdk.MiddlewareContext) context.Context {
			return context.Background()
		}, "original", ""},
		{"fresh carrier has no input", func(ctx context.Context, mc *sdk.MiddlewareContext) context.Context {
			return sdk.NewContext(ctx, mc.InvocationContext)
		}, "", "without history"},
	} {
		for _, durableFirst := range []bool{true, false} {
			t.Run(tc.name+"/"+durableOrderName(durableFirst), func(t *testing.T) {
				received = ""
				app := sdk.FunctionApp()
				if durableFirst {
					app.Use(d)
				}
				app.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
					return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
						mc.SetInputString(replacement)
						return next(tc.derive(ctx, mc), mc)
					}
				}))
				if !durableFirst {
					app.Use(d)
				}
				mc := &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{TriggerType: string(OrchestrationTriggerType)}}
				mc.SetInputString(original)
				err := app.Compose(func(ctx context.Context, _ *sdk.MiddlewareContext) error {
					_, err := adapter(ctx, original)
					return err
				})(sdk.ContextWithMiddleware(context.Background(), mc), mc)
				if tc.wantErr != "" {
					if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
						t.Fatalf("error = %v, want %q", err, tc.wantErr)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if received != tc.want {
					t.Fatalf("replayed input = %q, want %q", received, tc.want)
				}
			})
		}
	}
}

func TestRegisteredAdapter_InputSource(t *testing.T) {
	d := Middleware(WithOrchestrator("HelloCities", helloCities))
	be, client := newEmulatorBackend(t)
	original := scheduleAndEncodeRequest(t, be, client, "HelloCities", nil)
	adapter := d.provided[0].Func.(func(context.Context, string) (string, error))

	for _, tc := range []struct {
		name   string
		makeMC func() *sdk.MiddlewareContext
		bound  string
		want   string
	}{
		{"no carrier falls back", nil, original, ""},
		{"no carrier validates bound input", nil, "not base64!", "decode base64"},
		{"typed nil carrier falls back", func() *sdk.MiddlewareContext { return nil }, original, ""},
		{"unchanged carrier", func() *sdk.MiddlewareContext { m := &sdk.MiddlewareContext{}; m.SetInputString(original); return m }, original, ""},
		{"byte-backed carrier", func() *sdk.MiddlewareContext {
			m := &sdk.MiddlewareContext{}
			m.SetInputBytes([]byte(original))
			return m
		}, "stale!", ""},
		{"empty replacement must not fall back", func() *sdk.MiddlewareContext {
			m := &sdk.MiddlewareContext{}
			m.SetInputString(original)
			m.SetInputString("")
			return m
		}, original, "without history"},
		{"malformed replacement must not fall back", func() *sdk.MiddlewareContext {
			m := &sdk.MiddlewareContext{}
			m.SetInputString("not base64!")
			return m
		}, original, "decode base64"},
		{"truncated protobuf must not fall back", func() *sdk.MiddlewareContext { m := &sdk.MiddlewareContext{}; m.SetInputString("CgU="); return m }, original, "parse orchestrator request"},
		{"carrier repairs invalid bound text", func() *sdk.MiddlewareContext { m := &sdk.MiddlewareContext{}; m.SetInputString(original); return m }, "stale!", ""},
		{"cleared carrier rejects bound input", func() *sdk.MiddlewareContext {
			m := &sdk.MiddlewareContext{}
			m.SetInputString(original)
			_ = m.InputBytes()
			m.SetInputString("")
			m.SetInputBytes(nil)
			return m
		}, original, "without history"},
		// PR 1 keeps the old accessor semantics. Clearing only the string after
		// materializing bytes still exposes the cached bytes through InputString.
		// Defining cross-representation replacement is separate SDK work.
		{"cached byte fallback is unchanged", func() *sdk.MiddlewareContext {
			m := &sdk.MiddlewareContext{}
			m.SetInputString(original)
			_ = m.InputBytes()
			m.SetInputString("")
			return m
		}, "stale!", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.makeMC != nil {
				ctx = sdk.ContextWithMiddleware(ctx, tc.makeMC())
			}
			got, err := adapter(ctx, tc.bound)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) || got != "" {
					t.Fatalf("response=%q error=%v; want %q and no response", got, err, tc.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want, err := d.runner.loadAndRun(context.Background(), original)
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatal("adapter did not replay the selected input")
			}
		})
	}
}

func TestDurableWrap_OrchestrationDelegatesWithoutInjectingClient(t *testing.T) {
	d := Middleware(WithClient(&Client{}))
	mc := &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{
		TriggerType: string(OrchestrationTriggerType),
	}}
	want := errors.New("downstream failure")
	calls := 0
	err := d.Wrap(func(ctx context.Context, got *sdk.MiddlewareContext) error {
		calls++
		if got != mc {
			t.Error("middleware context changed")
		}
		if _, ok := ClientFromContext(ctx); ok {
			t.Error("Durable must not inject a management client into replay")
		}
		return want
	})(context.Background(), mc)
	if calls != 1 || !errors.Is(err, want) {
		t.Fatalf("calls=%d err=%v; want one delegation and the downstream error", calls, err)
	}
}

func TestRegisteredAdapter_RespectsMiddlewareShortCircuit(t *testing.T) {
	for _, durableFirst := range []bool{true, false} {
		t.Run(durableOrderName(durableFirst), func(t *testing.T) {
			calls := 0
			d := Middleware(WithOrchestrator("Blocked", func(*task.OrchestrationContext) (any, error) {
				calls++
				return "must not execute", nil
			}))
			be, client := newEmulatorBackend(t)
			encoded := scheduleAndEncodeRequest(t, be, client, "Blocked", nil)
			app := sdk.FunctionApp()
			if durableFirst {
				app.Use(d)
			}
			want := errors.New("rejected by middleware")
			app.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
				return func(context.Context, *sdk.MiddlewareContext) error { return want }
			}))
			if !durableFirst {
				app.Use(d)
			}
			var handlerCalls int
			mc := &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{TriggerType: string(OrchestrationTriggerType)}}
			err := app.Compose(func(ctx context.Context, mc *sdk.MiddlewareContext) error {
				handlerCalls++
				_, err := d.provided[0].Func.(func(context.Context, string) (string, error))(ctx, encoded)
				return err
			})(context.Background(), mc)
			if !errors.Is(err, want) || calls != 0 || handlerCalls != 0 {
				t.Fatalf("err=%v orchestratorCalls=%d handlerCalls=%d; expected rejection before execution", err, calls, handlerCalls)
			}
		})
	}
}

func TestRegisteredAdapter_ReturnsDecodeErrorsThroughMiddleware(t *testing.T) {
	for _, input := range []string{"not base64!", "", "CgFp", "CgU="} { // instanceId-only or truncated protobuf
		t.Run(input, func(t *testing.T) {
			d := Middleware(WithOrchestrator("HelloCities", helloCities))
			app := sdk.FunctionApp()
			app.Use(d)
			var steps []string
			var observed error
			app.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
				return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
					steps = append(steps, "before")
					observed = next(ctx, mc)
					steps = append(steps, "after")
					return observed
				}
			}))
			adapter := d.provided[0].Func.(func(context.Context, string) (string, error))
			err := app.Compose(func(ctx context.Context, mc *sdk.MiddlewareContext) error {
				_, err := adapter(ctx, input)
				return err
			})(context.Background(), &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{TriggerType: string(OrchestrationTriggerType)}})
			if err == nil || !errors.Is(err, observed) || !reflect.DeepEqual(steps, []string{"before", "after"}) {
				t.Fatalf("err=%v observed=%v steps=%v", err, observed, steps)
			}
			want := "without history"
			if input == "not base64!" {
				want = "decode base64"
			} else if input == "CgU=" {
				want = "parse orchestrator request"
			}
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error %q does not contain %q", err, want)
			}
		})
	}
}

func durableOrderName(durableFirst bool) string {
	if durableFirst {
		return "durable-first"
	}
	return "durable-last"
}

func TestRegisteredAdapters_UseTheirOwningRegistry(t *testing.T) {
	// Two Durable instances must not steal each other's orchestration invocations.
	// Both register functions, but each adapter closes over its own runner.
	var firstCalls, secondCalls int
	a := Middleware(WithOrchestrator("First", func(*task.OrchestrationContext) (any, error) {
		firstCalls++
		return "first", nil
	}))
	b := Middleware(WithOrchestrator("Second", func(*task.OrchestrationContext) (any, error) {
		secondCalls++
		return "second", nil
	}))
	app := sdk.FunctionApp()
	app.Use(a)
	app.Use(b)
	be, client := newEmulatorBackend(t)
	encoded := scheduleAndEncodeRequest(t, be, client, "Second", "")
	var f func(context.Context, string) (string, error)
	app.GetRegisteredFunctions().Range(func(_, value any) bool {
		r := value.(*sdk.RegisteredFunction)
		if r.FuncName == "Second" {
			f, _ = r.Func.(func(context.Context, string) (string, error))
		}
		return true
	})
	if f == nil {
		t.Fatal("missing callable adapter")
	}
	mc := &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{
		TriggerType: string(OrchestrationTriggerType),
	}}
	mc.SetInputString(encoded)
	if err := app.Compose(func(ctx context.Context, mc *sdk.MiddlewareContext) error {
		_, err := f(ctx, encoded)
		return err
	})(sdk.ContextWithMiddleware(context.Background(), mc), mc); err != nil {
		t.Fatalf("second registry could not replay its function: %v", err)
	}
	if firstCalls != 0 || secondCalls != 1 {
		t.Fatalf("wrong registry executed: first=%d second=%d", firstCalls, secondCalls)
	}
}

func TestRegisteredAdapter_PreservesFunctionIdentityAndBinding(t *testing.T) {
	d := Middleware(WithOrchestrator("HelloCities", helloCities))
	app := sdk.FunctionApp()
	app.Use(d)
	// The former placeholder had a byte parameter and only an error result.
	// Handler shape must not alter the name, ID, or trigger sent to the host.
	baseline := sdk.FunctionApp().RegisterFunction("HelloCities",
		func(context.Context, []byte) error { return nil }, orchestrationTriggerBinding{})
	app.GetRegisteredFunctions().Range(func(_, value any) bool {
		got := value.(*sdk.RegisteredFunction)
		if got.FuncName != baseline.FuncName || got.FuncId != baseline.FuncId || got.TriggerType != baseline.TriggerType || !reflect.DeepEqual(got.RawBindings, baseline.RawBindings) {
			t.Errorf("metadata changed: got=%+v baseline=%+v", got, baseline)
		}
		if got.ScriptFile == "" || got.ScriptFile == "<autogenerated>" {
			t.Errorf("adapter has no useful diagnostic source: %q", got.ScriptFile)
		}
		return true
	})
}
