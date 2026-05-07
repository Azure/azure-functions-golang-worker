package otelfunc

import (
	"context"
	"errors"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// newTestProvider returns a TracerProvider that records spans into the
// returned in-memory exporter for assertion. Uses a SimpleSpanProcessor so
// spans appear synchronously after span.End().
func newTestProvider() (*sdktrace.TracerProvider, *tracetest.InMemoryExporter) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	return tp, exp
}

func TestMiddleware_StartsServerSpan(t *testing.T) {
	tp, exp := newTestProvider()
	mw := Middleware(WithTracerProvider(tp))

	called := false
	chain := mw.Wrap(func(ctx context.Context, ic *sdk.InvocationContext) error {
		called = true
		// The user handler must observe a ctx that carries the just-started span.
		if !trace.SpanFromContext(ctx).SpanContext().IsValid() {
			t.Error("expected an active span on ctx inside the inner handler")
		}
		return nil
	})

	ic := &sdk.InvocationContext{
		InvocationID: "inv-1",
		FunctionName: "Hello",
		TriggerType:  "httpTrigger",
	}
	if err := chain(context.Background(), ic); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("inner handler not called")
	}

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 recorded span, got %d", len(spans))
	}
	span := spans[0]

	if span.Name != "Hello" {
		t.Errorf("span name = %q, want %q", span.Name, "Hello")
	}
	if span.SpanKind != trace.SpanKindServer {
		t.Errorf("span kind = %v, want Server", span.SpanKind)
	}

	wantAttrs := map[attribute.Key]string{
		"faas.invocation_id": "inv-1",
		"faas.name":          "Hello",
		"faas.trigger":       "http",
	}
	for _, a := range span.Attributes {
		if want, ok := wantAttrs[a.Key]; ok {
			if a.Value.AsString() != want {
				t.Errorf("attr %q = %q, want %q", a.Key, a.Value.AsString(), want)
			}
			delete(wantAttrs, a.Key)
		}
	}
	if len(wantAttrs) != 0 {
		t.Errorf("missing expected span attributes: %v", wantAttrs)
	}
}

func TestMiddleware_RecordsErrorOnFailure(t *testing.T) {
	tp, exp := newTestProvider()
	mw := Middleware(WithTracerProvider(tp))

	wantErr := errors.New("boom")
	chain := mw.Wrap(func(ctx context.Context, ic *sdk.InvocationContext) error {
		return wantErr
	})

	gotErr := chain(context.Background(), &sdk.InvocationContext{InvocationID: "x", FunctionName: "Fail"})
	if !errors.Is(gotErr, wantErr) {
		t.Errorf("expected error to propagate; got %v", gotErr)
	}

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 recorded span, got %d", len(spans))
	}
	if spans[0].Status.Code != codes.Error {
		t.Errorf("expected span status Error; got %v", spans[0].Status)
	}
	if len(spans[0].Events) == 0 {
		t.Errorf("expected RecordError to add an event to the span; got none")
	}
}

func TestMiddleware_ExtractsParentTraceContext(t *testing.T) {
	tp, exp := newTestProvider()
	mw := Middleware(
		WithTracerProvider(tp),
		WithPropagator(propagation.TraceContext{}),
	)

	// W3C trace parent: version 00, trace id 0123..., span id ab12..., flags 01.
	const traceParent = "00-0123456789abcdef0123456789abcdef-abcdef0123456789-01"

	chain := mw.Wrap(func(ctx context.Context, ic *sdk.InvocationContext) error { return nil })

	ic := &sdk.InvocationContext{
		InvocationID: "inv-tp",
		FunctionName: "Trace",
		TraceContext: sdk.TraceContext{TraceParent: traceParent},
	}
	if err := chain(context.Background(), ic); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	parentSC := spans[0].Parent
	if !parentSC.IsValid() {
		t.Fatal("expected the recorded span to have a valid remote parent SpanContext")
	}
	if parentSC.TraceID().String() != "0123456789abcdef0123456789abcdef" {
		t.Errorf("parent trace id mismatch: %s", parentSC.TraceID())
	}
	if parentSC.SpanID().String() != "abcdef0123456789" {
		t.Errorf("parent span id mismatch: %s", parentSC.SpanID())
	}
}

func TestMiddleware_ForceFlushAfterEachInvocation(t *testing.T) {
	tp, _ := newTestProvider()
	flusher := &countingFlusher{}
	mw := Middleware(WithTracerProvider(tp), WithFlusher(flusher))

	chain := mw.Wrap(func(ctx context.Context, ic *sdk.InvocationContext) error { return nil })

	for i := 0; i < 3; i++ {
		if err := chain(context.Background(), &sdk.InvocationContext{InvocationID: "i", FunctionName: "F"}); err != nil {
			t.Fatalf("invocation %d failed: %v", i, err)
		}
	}
	if flusher.calls != 3 {
		t.Errorf("ForceFlush called %d times; want 3", flusher.calls)
	}
}

func TestMiddleware_DefaultFlusherIsTracerProvider(t *testing.T) {
	// When no flusher is configured explicitly, the Middleware must use
	// the TracerProvider as the flusher (since sdktrace.TracerProvider
	// implements ForceFlush). Verifies users get safe behavior on
	// consumption plans without having to read the godoc carefully.
	tp := &spyTracerProvider{TracerProvider: mustNewTestProvider(t)}
	mw := Middleware(WithTracerProvider(tp))
	chain := mw.Wrap(func(ctx context.Context, ic *sdk.InvocationContext) error { return nil })

	if err := chain(context.Background(), &sdk.InvocationContext{FunctionName: "F"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tp.flushCalls != 1 {
		t.Errorf("expected default Flusher to call TracerProvider.ForceFlush; got %d calls", tp.flushCalls)
	}
}

func TestMiddleware_WithoutFlusher(t *testing.T) {
	// WithoutFlusher must override the auto-flush default.
	tp := &spyTracerProvider{TracerProvider: mustNewTestProvider(t)}
	mw := Middleware(WithTracerProvider(tp), WithoutFlusher())
	chain := mw.Wrap(func(ctx context.Context, ic *sdk.InvocationContext) error { return nil })

	if err := chain(context.Background(), &sdk.InvocationContext{FunctionName: "F"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tp.flushCalls != 0 {
		t.Errorf("expected no flush calls when WithoutFlusher is set; got %d", tp.flushCalls)
	}
}

func TestMiddleware_FlusherErrorDoesNotMaskUserError(t *testing.T) {
	tp, _ := newTestProvider()
	flusher := &countingFlusher{err: errors.New("flush failed")}
	mw := Middleware(WithTracerProvider(tp), WithFlusher(flusher))

	wantErr := errors.New("user")
	chain := mw.Wrap(func(ctx context.Context, ic *sdk.InvocationContext) error { return wantErr })

	gotErr := chain(context.Background(), &sdk.InvocationContext{FunctionName: "F"})
	if !errors.Is(gotErr, wantErr) {
		t.Errorf("expected user error to propagate, not flush error; got %v", gotErr)
	}
}

func TestMiddleware_CustomSpanNameFormatter(t *testing.T) {
	tp, exp := newTestProvider()
	mw := Middleware(
		WithTracerProvider(tp),
		WithSpanNameFormatter(func(ic *sdk.InvocationContext) string {
			return "fn:" + ic.FunctionName
		}),
	)
	chain := mw.Wrap(func(ctx context.Context, ic *sdk.InvocationContext) error { return nil })

	if err := chain(context.Background(), &sdk.InvocationContext{FunctionName: "Hello"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	spans := exp.GetSpans()
	if len(spans) != 1 || spans[0].Name != "fn:Hello" {
		t.Errorf("unexpected span: %+v", spans)
	}
}

func TestMiddleware_ExtraAttributes(t *testing.T) {
	tp, exp := newTestProvider()
	mw := Middleware(
		WithTracerProvider(tp),
		WithAttributes(attribute.String("deployment.slot", "prod")),
	)
	chain := mw.Wrap(func(ctx context.Context, ic *sdk.InvocationContext) error { return nil })

	if err := chain(context.Background(), &sdk.InvocationContext{FunctionName: "F"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	found := false
	for _, a := range spans[0].Attributes {
		if a.Key == "deployment.slot" && a.Value.AsString() == "prod" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected deployment.slot=prod on span attributes; got %+v", spans[0].Attributes)
	}
}

func TestClassifyTrigger(t *testing.T) {
	cases := map[string]string{
		"httpTrigger":             "http",
		"timerTrigger":            "timer",
		"blobTrigger":             "datasource",
		"cosmosDBTrigger":         "datasource",
		"eventHubTrigger":         "pubsub",
		"serviceBusTrigger":       "pubsub",
		"serviceBusQueueTrigger":  "pubsub",
		"serviceBusTopicTrigger":  "pubsub",
		"eventGridTrigger":        "pubsub",
		"":                        "other",
		"someUnknownTriggerType":  "other",
	}
	for input, want := range cases {
		if got := classifyTrigger(input); got != want {
			t.Errorf("classifyTrigger(%q) = %q, want %q", input, got, want)
		}
	}
}

// countingFlusher is a minimal Flusher used to assert the middleware calls
// ForceFlush once per invocation.
type countingFlusher struct {
	calls int
	err   error
}

func (c *countingFlusher) ForceFlush(context.Context) error {
	c.calls++
	return c.err
}

// mustNewTestProvider returns a recording TracerProvider for tests that
// don't care about the recorded spans (only about flush behavior).
func mustNewTestProvider(t *testing.T) *sdktrace.TracerProvider {
	t.Helper()
	tp, _ := newTestProvider()
	return tp
}

// spyTracerProvider wraps a real TracerProvider and counts ForceFlush
// invocations. Used to assert that the Middleware's default behavior
// (auto-flush via the TracerProvider) is wired correctly.
type spyTracerProvider struct {
	*sdktrace.TracerProvider
	flushCalls int
}

func (s *spyTracerProvider) ForceFlush(ctx context.Context) error {
	s.flushCalls++
	return s.TracerProvider.ForceFlush(ctx)
}
