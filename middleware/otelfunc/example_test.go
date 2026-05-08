package otelfunc_test

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/azure/azure-functions-golang-worker/middleware/otelfunc"
	"github.com/azure/azure-functions-golang-worker/sdk"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Example shows a minimal end-to-end Functions app with OpenTelemetry
// distributed tracing wired via otelfunc.Middleware.
//
// The user's HTTP handler is unchanged — middleware adds tracing without
// any change to the handler signature. Spans automatically correlate with
// the host's parent span via the W3C trace context the host attaches to
// every InvocationRequest.
func Example() {
	// 1. Build a TracerProvider. In real apps replace stdouttrace with
	//    your exporter of choice (OTLP, Application Insights, Jaeger, ...).
	exp, err := stdouttrace.New(stdouttrace.WithWriter(os.Stdout))
	if err != nil {
		panic(err)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	// 2. Wire the global TracerProvider and propagator. The Functions host
	//    sends W3C trace context, so propagation.TraceContext{} is the
	//    right default. Add propagation.Baggage{} if you also want baggage.
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// 3. Register otelfunc.Middleware. By default the Middleware uses the
	//    configured TracerProvider as its Flusher and force-flushes after
	//    every invocation -- critical on consumption-style plans (Flex,
	//    Linux Consumption) where the container can be frozen between
	//    invocations and lose buffered batches. Pass otelfunc.WithoutFlusher()
	//    to opt out on always-warm plans.
	app := sdk.FunctionApp()
	app.Use(otelfunc.Middleware())

	// 4. Register handlers as usual. They become traced automatically.
	app.HTTP("hello", helloHandler)

	// In a real app this would be:
	//     worker.Start(app)
	_ = app
}

// helloHandler is a normal Go HTTP handler. It uses the otel.Tracer to
// create a child span — the parent is the server-kind span otelfunc
// started for this invocation, and that one's parent is the host's span.
func helloHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := otel.Tracer("my-app").Start(r.Context(), "render-greeting")
	defer span.End()

	greeting := lookupGreeting(ctx)
	slog.InfoContext(ctx, "greeting computed", "greeting", greeting)

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(greeting))
}

func lookupGreeting(ctx context.Context) string {
	_, span := otel.Tracer("my-app").Start(ctx, "lookupGreeting",
		trace.WithSpanKind(trace.SpanKindInternal))
	defer span.End()
	return "hello"
}

// ExampleMiddleware_customAttributes shows how to attach static deployment-
// scoped attributes to every span. Useful for slot/region/build tags that
// aren't visible to the standard Resource detector.
func ExampleMiddleware_customAttributes() {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	app := sdk.FunctionApp()
	app.Use(otelfunc.Middleware(
		otelfunc.WithTracerProvider(tp),
		otelfunc.WithAttributes(
		// attribute.String("deployment.slot", os.Getenv("DEPLOYMENT_SLOT")),
		),
	))
	_ = app
}

// ExampleMiddleware_inboundBaggage shows the recommended pattern for
// reading inbound baggage and propagating it onward to downstream calls.
//
// otelfunc hydrates ctx with the host-supplied baggage automatically, so
// user code only needs the standard go.opentelemetry.io/otel/baggage API.
// Standard instrumentation libraries (otelhttp, otelgrpc) read baggage off
// ctx at call time and emit the W3C baggage header without extra wiring.
func ExampleMiddleware_inboundBaggage() {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	otel.SetTracerProvider(tp)
	// Add Baggage{} so otel.GetTextMapPropagator() also handles baggage.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	app := sdk.FunctionApp()
	app.Use(otelfunc.Middleware())

	app.HTTP("hello", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// 1. Read whatever baggage upstream attached.
		bag := baggage.FromContext(ctx)
		if tenant := bag.Member("tenant").Value(); tenant != "" {
			slog.InfoContext(ctx, "serving tenant", "tenant", tenant)
		}

		// 2. Add a baggage member of our own. The new value will travel
		//    on every outbound HTTP/gRPC call we make below.
		userMember, _ := baggage.NewMemberRaw("user_id", "u-42")
		newBag, _ := bag.SetMember(userMember)
		ctx = baggage.ContextWithBaggage(ctx, newBag)

		// 3. Make an outbound call. otelhttp.NewTransport reads baggage
		//    off ctx at call time.
		//
		//    client := &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
		//    resp, err := client.Do(req.WithContext(ctx))

		_ = ctx
		w.WriteHeader(http.StatusOK)
	})
	_ = app
}

// ExampleInvocationContext_outboundTraceAttributes shows the niche use
// case for ic.OutboundTraceAttributes: tagging the host's parent activity
// span (the one that becomes a "request" record in App Insights). Most
// users do not need this — span attributes set via span.SetAttributes are
// exported by the worker's own TracerProvider and land in the same OTel
// backend.
//
// Use this only when you need a tag to appear on the host's "requests"
// table specifically (e.g. for a KQL filter like
// `requests | where customDimensions.tenant == "contoso"`).
func ExampleInvocationContext_outboundTraceAttributes() {
	app := sdk.FunctionApp()
	app.HTTP("hello", func(w http.ResponseWriter, r *http.Request) {
		ic, _ := sdk.FromContext(r.Context())

		// Standard OTel pattern for span attrs (worker span -> App Insights):
		span := trace.SpanFromContext(r.Context())
		span.SetAttributes(attribute.String("user.id", "u-42"))

		// Niche escape hatch for host parent-span tags:
		if ic != nil {
			ic.OutboundTraceAttributes = map[string]string{
				"tenant":      r.Header.Get("X-Tenant"),
				"result.kind": "ok",
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	_ = app
}
