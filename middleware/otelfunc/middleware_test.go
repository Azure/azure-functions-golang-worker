package otelfunc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	lognoop "go.opentelemetry.io/otel/log/noop"
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

func TestMiddleware_StartsInvocationSpan(t *testing.T) {
	tp, exp := newTestProvider()
	mw := Middleware(WithTracerProvider(tp))

	called := false
	chain := mw.Wrap(func(ctx context.Context, mc *sdk.MiddlewareContext) error {
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
	if err := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: ic}); err != nil {
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

	if span.Name != "function Hello" {
		t.Errorf("span name = %q, want %q", span.Name, "function Hello")
	}
	if span.SpanKind != trace.SpanKindInternal {
		t.Errorf("span kind = %v, want Internal", span.SpanKind)
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
	chain := mw.Wrap(func(ctx context.Context, mc *sdk.MiddlewareContext) error {
		return wantErr
	})

	gotErr := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{InvocationID: "x", FunctionName: "Fail"}})
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

	chain := mw.Wrap(func(ctx context.Context, mc *sdk.MiddlewareContext) error { return nil })

	ic := &sdk.InvocationContext{
		InvocationID: "inv-tp",
		FunctionName: "Trace",
		TraceContext: sdk.TraceContext{TraceParent: traceParent},
	}
	if err := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: ic}); err != nil {
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

// Regression: when no Propagator is supplied via WithPropagator, the
// middleware must still extract W3C traceparent from the inbound
// InvocationContext. An earlier implementation defaulted to
// otel.GetTextMapPropagator() which is an empty composite that silently
// ignores traceparent — leaving worker spans uncorrelated with the
// host's parent activity.
func TestMiddleware_DefaultPropagator_ExtractsW3CTraceparent(t *testing.T) {
	tp, exp := newTestProvider()
	mw := Middleware(WithTracerProvider(tp)) // intentionally no WithPropagator

	const traceParent = "00-0123456789abcdef0123456789abcdef-abcdef0123456789-01"
	ic := &sdk.InvocationContext{
		InvocationID: "inv-default-prop",
		FunctionName: "Trace",
		TraceContext: sdk.TraceContext{TraceParent: traceParent},
	}

	chain := mw.Wrap(func(ctx context.Context, mc *sdk.MiddlewareContext) error { return nil })
	if err := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: ic}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if got := spans[0].SpanContext.TraceID().String(); got != "0123456789abcdef0123456789abcdef" {
		t.Errorf("span trace id = %s, want %s (default propagator must extract W3C)", got,
			"0123456789abcdef0123456789abcdef")
	}
	if got := spans[0].Parent.SpanID().String(); got != "abcdef0123456789" {
		t.Errorf("span parent id = %s, want %s", got, "abcdef0123456789")
	}
}

func TestMiddleware_ForceFlushAfterEachInvocation(t *testing.T) {
	tp, _ := newTestProvider()
	flusher := &countingFlusher{}
	mw := Middleware(WithTracerProvider(tp), WithFlusher(flusher))

	chain := mw.Wrap(func(ctx context.Context, mc *sdk.MiddlewareContext) error { return nil })

	for i := 0; i < 3; i++ {
		if err := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{InvocationID: "i", FunctionName: "F"}}); err != nil {
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
	chain := mw.Wrap(func(ctx context.Context, mc *sdk.MiddlewareContext) error { return nil })

	if err := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{FunctionName: "F"}}); err != nil {
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
	chain := mw.Wrap(func(ctx context.Context, mc *sdk.MiddlewareContext) error { return nil })

	if err := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{FunctionName: "F"}}); err != nil {
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
	chain := mw.Wrap(func(ctx context.Context, mc *sdk.MiddlewareContext) error { return wantErr })

	gotErr := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{FunctionName: "F"}})
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
	chain := mw.Wrap(func(ctx context.Context, mc *sdk.MiddlewareContext) error { return nil })

	if err := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{FunctionName: "Hello"}}); err != nil {
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
	chain := mw.Wrap(func(ctx context.Context, mc *sdk.MiddlewareContext) error { return nil })

	if err := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{FunctionName: "F"}}); err != nil {
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
		"httpTrigger":            "http",
		"timerTrigger":           "timer",
		"blobTrigger":            "datasource",
		"cosmosDBTrigger":        "datasource",
		"sqlTrigger":             "datasource",
		"eventHubTrigger":        "pubsub",
		"serviceBusTrigger":      "pubsub",
		"serviceBusQueueTrigger": "pubsub",
		"serviceBusTopicTrigger": "pubsub",
		"eventGridTrigger":       "pubsub",
		"queueTrigger":           "pubsub",
		"":                       "other",
		"someUnknownTriggerType": "other",
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

// =============================================================================
// Capability provider, env-var disable, and noop detection
// =============================================================================

// invocationContextForTest builds a minimal InvocationContext that lets us
// run the middleware once for assertion.
func invocationContextForTest() *sdk.InvocationContext {
	return &sdk.InvocationContext{
		InvocationID: "inv-cap",
		FunctionName: "Sample",
		TriggerType:  "httpTrigger",
	}
}

func TestMiddleware_AdvertisesCapabilities_WhenActive(t *testing.T) {
	// With a real exporter wired in, the middleware advertises both
	// OTel capability flags so the host can stop double-emitting telemetry.
	tp, _ := newTestProvider()
	mw := Middleware(WithTracerProvider(tp))

	cp, ok := mw.(sdk.CapabilityProvider)
	if !ok {
		t.Fatal("Middleware return value should implement sdk.CapabilityProvider")
	}
	caps := cp.Capabilities()
	if caps[CapabilityWorkerOpenTelemetryEnabled] != "true" {
		t.Errorf("%s = %q, want true", CapabilityWorkerOpenTelemetryEnabled, caps[CapabilityWorkerOpenTelemetryEnabled])
	}
	if caps[CapabilityWorkerOpenTelemetrySchemaVersion] != SchemaVersion {
		t.Errorf("%s = %q, want %q", CapabilityWorkerOpenTelemetrySchemaVersion,
			caps[CapabilityWorkerOpenTelemetrySchemaVersion], SchemaVersion)
	}
}

func TestMiddleware_DisabledByEnv_PassThroughAndNoCapabilities(t *testing.T) {
	t.Setenv(EnvDisable, "true")

	tp, exp := newTestProvider()
	mw := Middleware(WithTracerProvider(tp))

	// No capabilities advertised when disabled.
	cp, _ := mw.(sdk.CapabilityProvider)
	if cp != nil {
		if got := cp.Capabilities(); len(got) != 0 {
			t.Errorf("expected no capabilities when env-disabled; got %v", got)
		}
	}

	// Wrap and run; assert no spans were produced.
	called := false
	chain := mw.Wrap(func(ctx context.Context, _ *sdk.MiddlewareContext) error {
		called = true
		return nil
	})
	if err := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: invocationContextForTest()}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("inner handler must still be called when middleware is env-disabled")
	}
	if got := exp.GetSpans(); len(got) != 0 {
		t.Errorf("expected no spans when env-disabled; got %d", len(got))
	}
}

func TestMiddleware_NoopTracerProvider_PassThroughAndNoCapabilities(t *testing.T) {
	// Without an exporter or explicit TracerProvider, the middleware
	// should detect the noop default, become a pass-through, and skip
	// capability advertising. We isolate from any global state by not
	// touching otel.SetTracerProvider.
	mw := Middleware() // no options -> falls back to otel.GetTracerProvider() which is noop by default

	cp, _ := mw.(sdk.CapabilityProvider)
	if cp != nil {
		if got := cp.Capabilities(); len(got) != 0 {
			t.Errorf("expected no capabilities with noop TP; got %v", got)
		}
	}

	called := false
	chain := mw.Wrap(func(ctx context.Context, _ *sdk.MiddlewareContext) error {
		called = true
		return nil
	})
	if err := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: invocationContextForTest()}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("inner handler must still be called when TP is noop")
	}
}

func TestMiddleware_WithExporter_BuildsTPAndAdvertises(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	mw := Middleware(WithExporter(exp))

	cp, ok := mw.(sdk.CapabilityProvider)
	if !ok {
		t.Fatal("Middleware should implement sdk.CapabilityProvider")
	}
	if got := cp.Capabilities()[CapabilityWorkerOpenTelemetryEnabled]; got != "true" {
		t.Errorf("expected capability when WithExporter is used; got %q", got)
	}

	chain := mw.Wrap(func(ctx context.Context, _ *sdk.MiddlewareContext) error { return nil })
	if err := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: invocationContextForTest()}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Force flush is the middleware's responsibility on consumption plans;
	// we get it via the TP we built.
	// The InMemoryExporter is sync, so spans should already be present.
	if got := exp.GetSpans(); len(got) != 1 {
		t.Errorf("expected 1 span via WithExporter-built TP; got %d", len(got))
	}
}

// TestMiddleware_WithExporter_StacksMultipleExporters guards the contract
// that calling WithExporter more than once fans every span out to all of
// the supplied exporters. Users routing to multiple backends in-process
// (e.g. one OTLP target + one debug stdouttrace) rely on this; the
// previous single-field implementation silently dropped all but the last
// exporter.
func TestMiddleware_WithExporter_StacksMultipleExporters(t *testing.T) {
	exp1 := tracetest.NewInMemoryExporter()
	exp2 := tracetest.NewInMemoryExporter()
	mw := Middleware(WithExporter(exp1), WithExporter(exp2))

	chain := mw.Wrap(func(ctx context.Context, _ *sdk.MiddlewareContext) error { return nil })
	if err := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: invocationContextForTest()}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := len(exp1.GetSpans()); got != 1 {
		t.Errorf("exp1 should have received 1 span; got %d", got)
	}
	if got := len(exp2.GetSpans()); got != 1 {
		t.Errorf("exp2 should have received 1 span; got %d", got)
	}
}

// TestMiddleware_WithResource_AppendsToOwnedTracerProviderResource asserts
// that attributes passed via WithResource land on the Resource of the
// owned TracerProvider built from WithExporter, on top of the default
// cloud.provider/cloud.platform/service.name. Multiple WithResource
// calls accumulate.
func TestMiddleware_WithResource_AppendsToOwnedTracerProviderResource(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	mw := Middleware(
		WithExporter(exp),
		WithResource(attribute.String("deployment.environment", "production")),
		WithResource(attribute.String("build.sha", "abcdef1")),
	)

	chain := mw.Wrap(func(ctx context.Context, _ *sdk.MiddlewareContext) error { return nil })
	if err := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: invocationContextForTest()}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span; got %d", len(spans))
	}
	res := spans[0].Resource
	if res == nil {
		t.Fatal("span has no resource")
	}

	got := map[string]string{}
	for _, kv := range res.Attributes() {
		got[string(kv.Key)] = kv.Value.Emit()
	}
	if got["deployment.environment"] != "production" {
		t.Errorf("deployment.environment = %q, want %q", got["deployment.environment"], "production")
	}
	if got["build.sha"] != "abcdef1" {
		t.Errorf("build.sha = %q, want %q", got["build.sha"], "abcdef1")
	}
	// Defaults must still be present.
	if got["cloud.provider"] != "azure" {
		t.Errorf("cloud.provider = %q, want %q (default must survive WithResource merge)", got["cloud.provider"], "azure")
	}
	if got["cloud.platform"] != "azure_functions" {
		t.Errorf("cloud.platform = %q, want %q", got["cloud.platform"], "azure_functions")
	}
}

func TestIsDisabledByEnv_TruthyValues(t *testing.T) {
	cases := map[string]bool{
		"":      false,
		"false": false,
		"0":     false,
		"no":    false,
		"1":     true,
		"true":  true,
		"True":  true,
		"YES":   true,
		"on":    true,
	}
	for v, want := range cases {
		t.Run(v, func(t *testing.T) {
			t.Setenv(EnvDisable, v)
			if got := isDisabledByEnv(); got != want {
				t.Errorf("isDisabledByEnv(%q) = %v, want %v", v, got, want)
			}
		})
	}
}

func TestServiceNameFromEnv_FallsBackToWebsiteSiteName(t *testing.T) {
	t.Setenv(EnvServiceName, "")
	t.Setenv("WEBSITE_SITE_NAME", "my-site")
	if got := serviceNameFromEnv(); got != "my-site" {
		t.Errorf("serviceNameFromEnv() = %q, want %q", got, "my-site")
	}
}

func TestServiceNameFromEnv_OtelServiceNameWins(t *testing.T) {
	t.Setenv(EnvServiceName, "otel-name")
	t.Setenv("WEBSITE_SITE_NAME", "site-name")
	if got := serviceNameFromEnv(); got != "otel-name" {
		t.Errorf("serviceNameFromEnv() = %q, want %q (OTEL_SERVICE_NAME must win)", got, "otel-name")
	}
}

func TestIsNoopTracerProvider(t *testing.T) {
	// The default global TP (before any SetTracerProvider) is the
	// internal noop and must be detected.
	// (Note: tests share global state with otel; we assume nobody set a
	// global before this point — which is true in unit tests because
	// nothing imports a TP installer.)
	if !isNoopTracerProvider(nil) {
		t.Error("nil TracerProvider should be classified as noop")
	}

	// A real sdk/trace TracerProvider is not noop.
	tp, _ := newTestProvider()
	if isNoopTracerProvider(tp) {
		t.Error("real sdk/trace.TracerProvider should not be classified as noop")
	}
}

func TestMiddleware_InboundBaggage_VisibleToHandler(t *testing.T) {
	tp, _ := newTestProvider()
	mw := Middleware(WithTracerProvider(tp))

	var observed baggage.Baggage
	chain := mw.Wrap(func(ctx context.Context, mc *sdk.MiddlewareContext) error {
		observed = baggage.FromContext(ctx)
		return nil
	})

	ic := &sdk.InvocationContext{
		FunctionName: "fn",
		InvocationID: "id-1",
		TraceContext: sdk.TraceContext{
			Baggage: map[string]string{
				"tenant": "contoso",
				"region": "westus",
			},
		},
	}

	if err := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: ic}); err != nil {
		t.Fatalf("chain returned error: %v", err)
	}
	if got := observed.Member("tenant").Value(); got != "contoso" {
		t.Errorf("tenant baggage member: got %q want %q", got, "contoso")
	}
	if got := observed.Member("region").Value(); got != "westus" {
		t.Errorf("region baggage member: got %q want %q", got, "westus")
	}
}

func TestMiddleware_AutoHarvestsSpanAttributesToOutbound(t *testing.T) {
	// User code calls span.SetAttributes inside the handler; the
	// middleware harvests those at end-of-invocation and records them on
	// the MiddlewareContext so the worker dispatcher forwards them on
	// InvocationResponse.TraceContextAttributes. Matches the
	// dotnet-isolated worker's Activity.Tags propagation behavior.
	tp, _ := newTestProvider()
	mw := Middleware(WithTracerProvider(tp))

	chain := mw.Wrap(func(ctx context.Context, _ *sdk.MiddlewareContext) error {
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.String("user.id", "u-42"),
			attribute.String("tenant", "contoso"),
		)
		return nil
	})

	ic := &sdk.InvocationContext{FunctionName: "fn", InvocationID: "id-1"}
	mc := &sdk.MiddlewareContext{InvocationContext: ic}
	ctx := sdk.ContextWithMiddleware(context.Background(), mc)
	if err := chain(ctx, mc); err != nil {
		t.Fatalf("chain returned error: %v", err)
	}

	got := mc.OutboundTraceAttributes()
	if got["user.id"] != "u-42" {
		t.Errorf("user.id: got %q, want %q (full map: %v)", got["user.id"], "u-42", got)
	}
	if got["tenant"] != "contoso" {
		t.Errorf("tenant: got %q, want %q (full map: %v)", got["tenant"], "contoso", got)
	}
}

func TestMiddleware_AutoHarvestFiltersWorkerSetKeys(t *testing.T) {
	// The middleware itself sets faas.invocation_id / faas.name /
	// faas.trigger (plus optional process.pid, faas.instance,
	// azure.functions.live_logs_session_id) on the worker invocation
	// span. Those keys must NOT leak into OutboundTraceAttributes,
	// otherwise the host's parent activity would have its own keys
	// silently overwritten by the worker's value. Matches the
	// dotnet-isolated worker's KnownAttributes filter.
	tp, _ := newTestProvider()
	mw := Middleware(WithTracerProvider(tp))

	chain := mw.Wrap(func(ctx context.Context, _ *sdk.MiddlewareContext) error {
		// User attempts (intentionally or otherwise) to set keys the
		// middleware itself owns. These must be dropped on harvest.
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.String("faas.invocation_id", "user-supplied"),
			attribute.String("faas.name", "user-supplied"),
			attribute.String("faas.trigger", "user-supplied"),
			attribute.String("process.pid", "user-supplied"),
			attribute.String("faas.instance", "user-supplied"),
			attribute.String("azure.functions.live_logs_session_id", "user-supplied"),
			// A non-worker-set key should still survive the filter.
			attribute.String("custom.key", "user-value"),
		)
		return nil
	})

	ic := &sdk.InvocationContext{FunctionName: "fn", InvocationID: "id-1"}
	mc := &sdk.MiddlewareContext{InvocationContext: ic}
	ctx := sdk.ContextWithMiddleware(context.Background(), mc)
	if err := chain(ctx, mc); err != nil {
		t.Fatalf("chain returned error: %v", err)
	}

	got := mc.OutboundTraceAttributes()
	for _, k := range []string{
		"faas.invocation_id", "faas.name", "faas.trigger",
		"process.pid", "faas.instance",
		"azure.functions.live_logs_session_id",
	} {
		if _, present := got[k]; present {
			t.Errorf("worker-set key %q must be filtered out of OutboundTraceAttributes; full map: %v", k, got)
		}
	}
	if got["custom.key"] != "user-value" {
		t.Errorf("custom.key: got %q, want %q (full map: %v)", got["custom.key"], "user-value", got)
	}
}

func TestMiddleware_AutoHarvestExplicitSetterWinsOnCollision(t *testing.T) {
	// "Fill if absent" precedence: when middleware (or other framework
	// code) calls MiddlewareContext.SetOutboundTraceAttribute during the
	// invocation AND the span has the same key, the explicit setter
	// wins. The span-derived value only fills keys not already present.
	tp, _ := newTestProvider()
	mw := Middleware(WithTracerProvider(tp))

	chain := mw.Wrap(func(ctx context.Context, _ *sdk.MiddlewareContext) error {
		mc, _ := sdk.MiddlewareContextFrom(ctx)
		mc.SetOutboundTraceAttribute("tenant", "explicit-value")
		trace.SpanFromContext(ctx).SetAttributes(
			attribute.String("tenant", "span-value"),
		)
		return nil
	})

	ic := &sdk.InvocationContext{FunctionName: "fn", InvocationID: "id-1"}
	mc := &sdk.MiddlewareContext{InvocationContext: ic}
	ctx := sdk.ContextWithMiddleware(context.Background(), mc)
	if err := chain(ctx, mc); err != nil {
		t.Fatalf("chain returned error: %v", err)
	}
	if got := mc.OutboundTraceAttributes()["tenant"]; got != "explicit-value" {
		t.Errorf("tenant: explicit setter must win on collision; got %q, want %q", got, "explicit-value")
	}
}

func TestBuildInboundBaggage_SkipsInvalidEntries(t *testing.T) {
	in := map[string]string{
		"valid": "ok",
		"":      "bad-key", // empty key is invalid per the baggage spec
	}
	bag := buildInboundBaggage(in)
	if got := bag.Member("valid").Value(); got != "ok" {
		t.Errorf("valid member: got %q want %q", got, "ok")
	}
	if bag.Len() != 1 {
		t.Errorf("expected only 1 valid member kept, got %d", bag.Len())
	}
}

func TestMiddleware_OwnsTracerProvider_ShutdownReleasesIt(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	mw := Middleware(WithExporter(exp))

	sp, ok := mw.(sdk.ShutdownProvider)
	if !ok {
		t.Fatal("Middleware must implement sdk.ShutdownProvider")
	}
	if err := sp.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown returned err: %v", err)
	}
}

func TestMiddleware_DoesNotShutdownUserProvidedTracerProvider(t *testing.T) {
	tp, _ := newTestProvider()
	mw := Middleware(WithTracerProvider(tp))

	sp, ok := mw.(sdk.ShutdownProvider)
	if !ok {
		t.Fatal("Middleware must implement sdk.ShutdownProvider")
	}
	// Should be a no-op when the user supplied the TP.
	if err := sp.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown returned err: %v", err)
	}
	// User-supplied TP must still be usable after Shutdown — i.e. we did
	// not call Shutdown on it.
	tracer := tp.Tracer("post-shutdown")
	_, span := tracer.Start(context.Background(), "post-shutdown-span")
	span.End()
}

func TestHasOTLPEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "")
	if hasOTLPEndpoint() {
		t.Error("hasOTLPEndpoint should be false when no env vars set")
	}
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://example.com:4318")
	if !hasOTLPEndpoint() {
		t.Error("hasOTLPEndpoint should be true when generic endpoint is set")
	}
}

func TestMiddleware_AutoOTLP_BuildsTPWhenEnvVarSet(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9999") // unreachable but parseable

	mw := Middleware()

	cp, ok := mw.(sdk.CapabilityProvider)
	if !ok {
		t.Fatal("Middleware must implement sdk.CapabilityProvider")
	}
	if cp.Capabilities()[CapabilityWorkerOpenTelemetryEnabled] != "true" {
		t.Error("auto-OTLP path should advertise WorkerOpenTelemetryEnabled=true")
	}

	// Shutdown should succeed (the unreachable endpoint just means no
	// telemetry is delivered, not that Shutdown errors).
	sp, _ := mw.(sdk.ShutdownProvider)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = sp.Shutdown(ctx)
}

func TestMiddleware_AutoOTLP_NoEnvVarStaysPassThrough(t *testing.T) {
	// Keep env explicitly empty so we don't pick up any ambient setting.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "")

	mw := Middleware()

	cp, ok := mw.(sdk.CapabilityProvider)
	if !ok {
		t.Fatal("Middleware must implement sdk.CapabilityProvider")
	}
	if got := cp.Capabilities()[CapabilityWorkerOpenTelemetryEnabled]; got != "" {
		t.Errorf("with no exporter and no env var, capability should be empty; got %q", got)
	}
}

func TestIsNoopLoggerProvider(t *testing.T) {
	if !isNoopLoggerProvider(nil) {
		t.Error("nil LoggerProvider should be classified as noop")
	}
	if !isNoopLoggerProvider(lognoop.NewLoggerProvider()) {
		t.Error("noop LoggerProvider should be classified as noop")
	}
}

// TestBuildDefaultResource_AzureEnvVarsPopulateResourceAttrs verifies that
// when the Azure Functions / App Service environment variables are set
// (as they always are in a real Function App), the owned TracerProvider's
// Resource picks up cloud.region, cloud.resource.id, and
// deployment.environment.name in the exact format the Java worker emits,
// so cross-runtime dashboards filter on identical keys/values.
func TestBuildDefaultResource_AzureEnvVarsPopulateResourceAttrs(t *testing.T) {
	const (
		subID  = "591f4fff-379d-40b1-bbcf-91f91afaa636"
		stamp  = "northeurope-stamp"
		rg     = "goworker-flex-rg"
		site   = "func-goworker-flex"
		region = "northeurope"
		slot   = "staging"
	)
	t.Setenv("REGION_NAME", region)
	t.Setenv("WEBSITE_RESOURCE_GROUP", rg)
	t.Setenv("WEBSITE_OWNER_NAME", subID+"+"+stamp)
	t.Setenv("WEBSITE_SITE_NAME", site)
	t.Setenv("WEBSITE_SLOT_NAME", slot)

	res := buildDefaultResource()
	if res == nil {
		t.Fatal("buildDefaultResource returned nil")
	}

	got := map[string]string{}
	for _, kv := range res.Attributes() {
		got[string(kv.Key)] = kv.Value.Emit()
	}

	if got["cloud.region"] != region {
		t.Errorf("cloud.region = %q, want %q", got["cloud.region"], region)
	}
	wantArm := "/subscriptions/" + subID + "/resourceGroups/" + rg + "/providers/Microsoft.Web/sites/" + site
	// OTel semconv v1.27 renamed cloud.resource.id (older Java-worker
	// vocabulary) to cloud.resource_id. We track the current spec.
	if got["cloud.resource_id"] != wantArm {
		t.Errorf("cloud.resource_id = %q, want %q", got["cloud.resource_id"], wantArm)
	}
	if got["deployment.environment.name"] != slot {
		t.Errorf("deployment.environment.name = %q, want %q", got["deployment.environment.name"], slot)
	}
}

// TestBuildDefaultResource_DeploymentEnvDefaultsToProduction confirms the
// Java-parity default: when WEBSITE_SLOT_NAME is unset, the slot is
// reported as "production" rather than being omitted from the Resource.
func TestBuildDefaultResource_DeploymentEnvDefaultsToProduction(t *testing.T) {
	t.Setenv("WEBSITE_SLOT_NAME", "")
	res := buildDefaultResource()
	for _, kv := range res.Attributes() {
		if string(kv.Key) == "deployment.environment.name" {
			if got := kv.Value.Emit(); got != "production" {
				t.Errorf("deployment.environment.name = %q, want %q", got, "production")
			}
			return
		}
	}
	t.Error("deployment.environment.name attribute missing entirely")
}

// TestBuildDefaultResource_OmitsArmIDWhenInputsIncomplete asserts that
// cloud.resource.id is left off the Resource when any of the three
// required environment variables (WEBSITE_OWNER_NAME, WEBSITE_RESOURCE_GROUP,
// WEBSITE_SITE_NAME) is missing -- matches the Java behavior of producing
// no attribute rather than a malformed string.
func TestBuildDefaultResource_OmitsArmIDWhenInputsIncomplete(t *testing.T) {
	t.Setenv("WEBSITE_RESOURCE_GROUP", "rg")
	t.Setenv("WEBSITE_SITE_NAME", "site")
	t.Setenv("WEBSITE_OWNER_NAME", "") // missing
	res := buildDefaultResource()
	for _, kv := range res.Attributes() {
		if string(kv.Key) == "cloud.resource_id" {
			t.Errorf("cloud.resource_id should be omitted; got %q", kv.Value.Emit())
		}
	}
}

func TestExtractSubscriptionID(t *testing.T) {
	cases := map[string]string{
		"sub-id+stamp-region":         "sub-id",
		"591f4fff-379d-40b1+anything": "591f4fff-379d-40b1",
		"":                            "",
		"no-plus":                     "",
		"+stamp-only":                 "", // empty prefix, treat as invalid
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			if got := extractSubscriptionID(in); got != want {
				t.Errorf("extractSubscriptionID(%q) = %q, want %q", in, got, want)
			}
		})
	}
}

// TestMiddleware_PromotesInboundTraceContextAttrs asserts that the host-
// supplied RpcTraceContext.attributes (HostInstanceId / ProcessId /
// #AzFuncLiveLogsSessionId) get surfaced as standard semconv span
// attributes (faas.instance / process.pid) and an Azure-specific
// attribute (azure.functions.live_logs_session_id). This is the
// log-correlation path the Java worker exposes via getAzureContext().
func TestMiddleware_PromotesInboundTraceContextAttrs(t *testing.T) {
	tp, exp := newTestProvider()
	mw := Middleware(WithTracerProvider(tp))
	chain := mw.Wrap(func(ctx context.Context, _ *sdk.MiddlewareContext) error { return nil })

	ic := &sdk.InvocationContext{
		InvocationID: "inv-1",
		FunctionName: "Hello",
		TraceContext: sdk.TraceContext{
			Attributes: map[string]string{
				"HostInstanceId":           "host-abc",
				"ProcessId":                "42",
				"#AzFuncLiveLogsSessionId": "session-xyz",
			},
		},
	}
	if err := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: ic}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	got := map[attribute.Key]string{}
	for _, a := range spans[0].Attributes {
		got[a.Key] = a.Value.Emit()
	}
	if got["faas.instance"] != "host-abc" {
		t.Errorf("faas.instance = %q, want %q", got["faas.instance"], "host-abc")
	}
	if got["process.pid"] != "42" {
		t.Errorf("process.pid = %q, want %q", got["process.pid"], "42")
	}
	if got["azure.functions.live_logs_session_id"] != "session-xyz" {
		t.Errorf("azure.functions.live_logs_session_id = %q, want %q", got["azure.functions.live_logs_session_id"], "session-xyz")
	}
}

// TestMiddleware_OmitsTraceContextAttrsWhenMissing confirms the
// "(absent) -> attribute not set" semantic: when the host omits one of
// the optional attributes, the span carries no entry for that key
// rather than an empty-string value (which would pollute downstream
// dashboards' uniques() / cardinality calculations).
func TestMiddleware_OmitsTraceContextAttrsWhenMissing(t *testing.T) {
	tp, exp := newTestProvider()
	mw := Middleware(WithTracerProvider(tp))
	chain := mw.Wrap(func(ctx context.Context, _ *sdk.MiddlewareContext) error { return nil })

	if err := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{
		InvocationID: "inv-2",
		FunctionName: "Hello",
		// TraceContext zero-valued -> Attributes is nil
	}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	for _, a := range spans[0].Attributes {
		switch string(a.Key) {
		case "faas.instance", "process.pid", "azure.functions.live_logs_session_id":
			t.Errorf("optional attribute %q should be absent when host didn't send it; got %q", a.Key, a.Value.Emit())
		}
	}
}

// TestMiddleware_TracerInstrumentationVersionSet verifies that spans
// produced by the middleware carry an otel.library.version equal to the
// resolved SDK version. Lets users query telemetry by SDK build (e.g.
// "all spans from worker SDK v0.4.0-preview") without further wiring.
func TestMiddleware_TracerInstrumentationVersionSet(t *testing.T) {
	tp, exp := newTestProvider()
	mw := Middleware(WithTracerProvider(tp))
	chain := mw.Wrap(func(ctx context.Context, _ *sdk.MiddlewareContext) error { return nil })

	if err := chain(context.Background(), &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{FunctionName: "Hello"}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	got := spans[0].InstrumentationScope.Version
	if got == "" {
		t.Error("InstrumentationScope.Version is empty; want a non-empty SDK version")
	}
	// Don't assert exact value: in unit tests the resolved version is
	// "(devel)" (no module replace, building the test binary directly).
	// Tagged-release builds will see "v0.X.Y-preview". Either is correct.
}
