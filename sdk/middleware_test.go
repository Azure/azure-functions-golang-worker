package sdk

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestApp_Use_RegistersMiddleware(t *testing.T) {
	// Verifies Use is wired into the chain by counting invocations of the
	// registered middleware when the composed chain runs. The middleware
	// slice is not exported, so we can't len() it directly; the public
	// behavior we care about is "registered middleware runs", which this
	// test asserts.
	app := FunctionApp()

	var calls int
	mw := MiddlewareFunc(func(next Handler) Handler {
		return func(ctx context.Context, ic *InvocationContext) error {
			calls++
			return next(ctx, ic)
		}
	})
	app.Use(mw)
	app.Use(mw)

	chain := app.Compose(func(ctx context.Context, ic *InvocationContext) error { return nil })
	if err := chain(context.Background(), &InvocationContext{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected the same Middleware registered twice to run twice; got %d call(s)", calls)
	}
}

func TestApp_Use_NilIgnored(t *testing.T) {
	// Nil Middleware must be silently dropped — confirming this lets users
	// register conditional middleware (mw := condition() ? real : nil)
	// without guarding the call.
	app := FunctionApp()
	app.Use(nil)

	called := false
	chain := app.Compose(func(ctx context.Context, ic *InvocationContext) error {
		called = true
		return nil
	})
	if err := chain(context.Background(), &InvocationContext{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("inner handler not called; nil registration may have broken the chain")
	}
}

func TestApp_Compose_ExecutionOrder(t *testing.T) {
	// First-registered middleware must be the outermost: it observes the
	// invocation first and last. This matches the chi/gin/echo convention
	// and gRPC interceptors.
	app := FunctionApp()
	var trace []string

	app.Use(MiddlewareFunc(func(next Handler) Handler {
		return func(ctx context.Context, ic *InvocationContext) error {
			trace = append(trace, "A:before")
			err := next(ctx, ic)
			trace = append(trace, "A:after")
			return err
		}
	}))
	app.Use(MiddlewareFunc(func(next Handler) Handler {
		return func(ctx context.Context, ic *InvocationContext) error {
			trace = append(trace, "B:before")
			err := next(ctx, ic)
			trace = append(trace, "B:after")
			return err
		}
	}))

	chain := app.Compose(func(ctx context.Context, ic *InvocationContext) error {
		trace = append(trace, "inner")
		return nil
	})
	if err := chain(context.Background(), &InvocationContext{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"A:before", "B:before", "inner", "B:after", "A:after"}
	if !reflect.DeepEqual(trace, want) {
		t.Errorf("ordering mismatch:\n got %v\nwant %v", trace, want)
	}
}

func TestApp_Compose_NoMiddleware(t *testing.T) {
	// With no middleware, the inner handler runs unwrapped — verifies
	// Compose doesn't add an unnecessary layer.
	app := FunctionApp()
	called := false

	chain := app.Compose(func(ctx context.Context, ic *InvocationContext) error {
		called = true
		return nil
	})
	if err := chain(context.Background(), &InvocationContext{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("inner handler not called when chain has no middleware")
	}
}

func TestApp_Compose_ErrorPropagation(t *testing.T) {
	app := FunctionApp()
	wantErr := errors.New("boom")
	app.Use(MiddlewareFunc(func(next Handler) Handler {
		return func(ctx context.Context, ic *InvocationContext) error {
			// Middleware passes the error through unchanged.
			return next(ctx, ic)
		}
	}))

	chain := app.Compose(func(ctx context.Context, ic *InvocationContext) error { return wantErr })
	if gotErr := chain(context.Background(), &InvocationContext{}); !errors.Is(gotErr, wantErr) {
		t.Errorf("expected wrapped error to propagate; got %v", gotErr)
	}
}

func TestApp_Compose_ShortCircuit(t *testing.T) {
	// A middleware can short-circuit by not calling next — verifies the
	// inner handler is not invoked.
	app := FunctionApp()
	gate := errors.New("gated")
	app.Use(MiddlewareFunc(func(next Handler) Handler {
		return func(ctx context.Context, ic *InvocationContext) error { return gate }
	}))

	innerCalled := false
	chain := app.Compose(func(ctx context.Context, ic *InvocationContext) error {
		innerCalled = true
		return nil
	})
	if err := chain(context.Background(), &InvocationContext{}); !errors.Is(err, gate) {
		t.Errorf("expected gate error from short-circuit; got %v", err)
	}
	if innerCalled {
		t.Error("inner handler must not be called when middleware short-circuits")
	}
}

func TestApp_Compose_ContextEnrichment(t *testing.T) {
	// Middleware can enrich ctx; the inner handler observes the enriched
	// ctx, not the original.
	app := FunctionApp()
	type ctxKey struct{}
	app.Use(MiddlewareFunc(func(next Handler) Handler {
		return func(ctx context.Context, ic *InvocationContext) error {
			ctx = context.WithValue(ctx, ctxKey{}, "enriched")
			return next(ctx, ic)
		}
	}))

	var observed string
	chain := app.Compose(func(ctx context.Context, ic *InvocationContext) error {
		if v, ok := ctx.Value(ctxKey{}).(string); ok {
			observed = v
		}
		return nil
	})
	if err := chain(context.Background(), &InvocationContext{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if observed != "enriched" {
		t.Errorf("inner handler did not observe middleware-enriched ctx; got %q", observed)
	}
}

// capProviderMW is a stub Middleware implementation that also implements
// [CapabilityProvider] for use by App.Use capability tests.
type capProviderMW struct {
	caps map[string]string
}

func (m *capProviderMW) Wrap(next Handler) Handler { return next }

func (m *capProviderMW) Capabilities() map[string]string { return m.caps }

func TestApp_Use_MergesCapabilitiesFromCapabilityProvider(t *testing.T) {
	// A Middleware that implements CapabilityProvider has its capability map
	// merged into App.Capabilities at registration time.
	app := FunctionApp()

	app.Use(&capProviderMW{caps: map[string]string{"WorkerOpenTelemetryEnabled": "true"}})
	app.Use(&capProviderMW{caps: map[string]string{"OtherCap": "yes"}})

	got := app.Capabilities()
	want := map[string]string{
		"WorkerOpenTelemetryEnabled": "true",
		"OtherCap":                   "yes",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("merged capabilities mismatch:\n got %v\nwant %v", got, want)
	}
}

func TestApp_Use_LaterCapabilityProviderWins(t *testing.T) {
	// Later registrations overwrite earlier values for the same key. This
	// matches the documented contract on App.Use.
	app := FunctionApp()
	app.Use(&capProviderMW{caps: map[string]string{"k": "first"}})
	app.Use(&capProviderMW{caps: map[string]string{"k": "second"}})

	if got := app.Capabilities()["k"]; got != "second" {
		t.Errorf("k = %q, want %q (later registration must win)", got, "second")
	}
}

func TestApp_Use_NonCapabilityProviderIgnored(t *testing.T) {
	// Plain MiddlewareFunc Middleware does not implement CapabilityProvider
	// and must not contribute to App.Capabilities.
	app := FunctionApp()
	app.Use(MiddlewareFunc(func(next Handler) Handler { return next }))

	if got := app.Capabilities(); len(got) != 0 {
		t.Errorf("plain middleware must not produce capabilities; got %v", got)
	}
}

func TestApp_Capabilities_ReturnsCopy(t *testing.T) {
	// App.Capabilities returns a copy callers may mutate without affecting
	// the App's internal map.
	app := FunctionApp()
	app.Use(&capProviderMW{caps: map[string]string{"k": "v"}})

	first := app.Capabilities()
	first["k"] = "mutated"
	first["new"] = "added"

	second := app.Capabilities()
	if second["k"] != "v" {
		t.Errorf("App's internal capabilities were mutated through the returned map: got %q, want %q", second["k"], "v")
	}
	if _, ok := second["new"]; ok {
		t.Errorf("App's internal capabilities gained a key through the returned map")
	}
}

func TestApp_Capabilities_EmptyMapWhenNone(t *testing.T) {
	// Returns a non-nil empty map when no CapabilityProvider middleware is
	// registered, so callers can range/len safely without nil checks.
	app := FunctionApp()
	got := app.Capabilities()
	if got == nil {
		t.Error("Capabilities returned nil; want empty map")
	}
	if len(got) != 0 {
		t.Errorf("Capabilities = %v, want empty map", got)
	}
}
