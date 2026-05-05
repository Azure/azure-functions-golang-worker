package otelfunc_test

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/otelfunc"

	"go.opentelemetry.io/otel"
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
