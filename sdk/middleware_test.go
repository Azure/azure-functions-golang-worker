package sdk

import (
	"context"
	"errors"
	"testing"
)

func TestApp_Use_Append(t *testing.T) {
	app := FunctionApp()
	if got := len(app.Middlewares()); got != 0 {
		t.Errorf("new app should have no middlewares, got %d", got)
	}

	mw := func(next Handler) Handler { return next }
	app.Use(mw)
	app.Use(mw)

	if got := len(app.Middlewares()); got != 2 {
		t.Errorf("expected 2 middlewares after two Use calls, got %d", got)
	}
}

func TestApp_Use_NilIgnored(t *testing.T) {
	app := FunctionApp()
	app.Use(nil)
	if got := len(app.Middlewares()); got != 0 {
		t.Errorf("registering nil middleware must not be recorded; got len=%d", got)
	}
}

func TestComposeMiddleware_ExecutionOrder(t *testing.T) {
	// First-registered middleware must be the outermost: it observes the
	// invocation first and last. This matches the chi/gin/echo convention
	// and gRPC interceptors.
	var trace []string

	mwA := func(next Handler) Handler {
		return func(ctx context.Context, ic *InvocationContext) error {
			trace = append(trace, "A:before")
			err := next(ctx, ic)
			trace = append(trace, "A:after")
			return err
		}
	}
	mwB := func(next Handler) Handler {
		return func(ctx context.Context, ic *InvocationContext) error {
			trace = append(trace, "B:before")
			err := next(ctx, ic)
			trace = append(trace, "B:after")
			return err
		}
	}
	inner := func(ctx context.Context, ic *InvocationContext) error {
		trace = append(trace, "inner")
		return nil
	}

	chain := ComposeMiddleware([]Middleware{mwA, mwB}, inner)
	if err := chain(context.Background(), &InvocationContext{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"A:before", "B:before", "inner", "B:after", "A:after"}
	if len(trace) != len(want) {
		t.Fatalf("trace length mismatch: got %v, want %v", trace, want)
	}
	for i, step := range want {
		if trace[i] != step {
			t.Errorf("step %d: got %q, want %q", i, trace[i], step)
		}
	}
}

func TestComposeMiddleware_NoMiddleware(t *testing.T) {
	// With no middleware, the inner handler runs unwrapped — verifies
	// ComposeMiddleware doesn't add an unnecessary layer.
	called := false
	inner := func(ctx context.Context, ic *InvocationContext) error {
		called = true
		return nil
	}

	chain := ComposeMiddleware(nil, inner)
	if err := chain(context.Background(), &InvocationContext{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("inner handler not called when chain has no middleware")
	}
}

func TestComposeMiddleware_ErrorPropagation(t *testing.T) {
	wantErr := errors.New("boom")
	inner := func(ctx context.Context, ic *InvocationContext) error {
		return wantErr
	}
	mw := func(next Handler) Handler {
		return func(ctx context.Context, ic *InvocationContext) error {
			// Middleware passes the error through unchanged.
			return next(ctx, ic)
		}
	}

	chain := ComposeMiddleware([]Middleware{mw}, inner)
	gotErr := chain(context.Background(), &InvocationContext{})
	if !errors.Is(gotErr, wantErr) {
		t.Errorf("expected wrapped error to propagate; got %v", gotErr)
	}
}

func TestComposeMiddleware_ShortCircuit(t *testing.T) {
	// A middleware can short-circuit by not calling next — verifies the
	// inner handler is not invoked.
	innerCalled := false
	inner := func(ctx context.Context, ic *InvocationContext) error {
		innerCalled = true
		return nil
	}
	gate := errors.New("gated")
	mw := func(next Handler) Handler {
		return func(ctx context.Context, ic *InvocationContext) error {
			return gate
		}
	}

	chain := ComposeMiddleware([]Middleware{mw}, inner)
	if err := chain(context.Background(), &InvocationContext{}); !errors.Is(err, gate) {
		t.Errorf("expected gate error from short-circuit; got %v", err)
	}
	if innerCalled {
		t.Error("inner handler must not be called when middleware short-circuits")
	}
}

func TestComposeMiddleware_ContextEnrichment(t *testing.T) {
	// Middleware can enrich ctx; the inner handler observes the enriched
	// ctx, not the original.
	type ctxKey struct{}
	mw := func(next Handler) Handler {
		return func(ctx context.Context, ic *InvocationContext) error {
			ctx = context.WithValue(ctx, ctxKey{}, "enriched")
			return next(ctx, ic)
		}
	}
	var observed string
	inner := func(ctx context.Context, ic *InvocationContext) error {
		if v, ok := ctx.Value(ctxKey{}).(string); ok {
			observed = v
		}
		return nil
	}

	chain := ComposeMiddleware([]Middleware{mw}, inner)
	if err := chain(context.Background(), &InvocationContext{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if observed != "enriched" {
		t.Errorf("inner handler did not observe middleware-enriched ctx; got %q", observed)
	}
}
