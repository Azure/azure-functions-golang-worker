// Package otelfunc provides an OpenTelemetry-tracing middleware for the
// azure-functions-golang-worker SDK.
//
// Apps that want distributed tracing import this package and register the
// Middleware via App.Use:
//
//	import (
//	    "github.com/azure/azure-functions-golang-worker/sdk"
//	    "github.com/azure/azure-functions-golang-worker/middleware/otelfunc"
//	)
//
//	func main() {
//	    tp := buildTracerProvider() // user-supplied
//	    otel.SetTracerProvider(tp)
//	    otel.SetTextMapPropagator(propagation.TraceContext{})
//
//	    app := sdk.FunctionApp()
//	    app.Use(otelfunc.Middleware())
//
//	    app.HTTP("hello", helloHandler)
//	    app.Timer("nightly", timerHandler)
//	    worker.Start(app)
//	}
//
// Design follows aws-lambda-go's otellambda package: a thin Middleware that
// (a) extracts the host's W3C trace context from sdk.InvocationContext,
// (b) starts a server-kind span around the user handler, and (c) force-flushes
// telemetry after each invocation because the Functions runtime may freeze
// the worker process between invocations on consumption SKUs (Flex
// Consumption, Linux Consumption).
//
// Force-flushing is on by default when the configured TracerProvider
// implements the [Flusher] interface (which the standard
// go.opentelemetry.io/otel/sdk/trace TracerProvider does). Override with
// [WithFlusher] or disable with [WithoutFlusher].
package otelfunc

import (
	"context"
	"errors"
	"log"

	"github.com/azure/azure-functions-golang-worker/sdk"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

// ScopeName is the OpenTelemetry instrumentation scope used by the Middleware
// when obtaining a Tracer from the configured TracerProvider.
const ScopeName = "github.com/azure/azure-functions-golang-worker/middleware/otelfunc"

// Flusher is the optional contract the Middleware uses to push pending
// telemetry to the configured exporter at the end of each invocation.
//
// The standard go.opentelemetry.io/otel/sdk/trace.TracerProvider satisfies
// Flusher via its ForceFlush method, so by default the Middleware uses the
// configured TracerProvider as the flusher (see [Middleware]). Pass
// [WithFlusher] to override, or [WithoutFlusher] to disable flushing.
//
// Force-flushing is not strictly required when running on plans that keep
// the worker process warm (Premium, Dedicated), but is essential on
// consumption-style plans (Flex Consumption, Linux Consumption) where the
// host may freeze the process between invocations and lose any buffered
// batches that the exporter has not yet shipped.
type Flusher interface {
	ForceFlush(ctx context.Context) error
}

// Option configures the Middleware. Pass options into Middleware to customize
// the TracerProvider, propagator, flusher, span name, or extra attributes.
type Option interface {
	apply(*config)
}

type optionFunc func(*config)

func (f optionFunc) apply(c *config) { f(c) }

// WithTracerProvider sets the OpenTelemetry TracerProvider used to obtain the
// Tracer. Defaults to otel.GetTracerProvider().
func WithTracerProvider(tp trace.TracerProvider) Option {
	return optionFunc(func(c *config) {
		if tp != nil {
			c.tp = tp
		}
	})
}

// WithPropagator sets the TextMapPropagator used to extract incoming W3C
// trace context from sdk.InvocationContext. Defaults to
// otel.GetTextMapPropagator(). Most callers should leave this at the default
// and configure the global propagator once at startup via
// otel.SetTextMapPropagator(propagation.TraceContext{}).
func WithPropagator(p propagation.TextMapPropagator) Option {
	return optionFunc(func(c *config) {
		if p != nil {
			c.propagator = p
		}
	})
}

// WithFlusher sets the [Flusher] called after every invocation, overriding
// the default behavior of using the configured TracerProvider when it
// satisfies the [Flusher] interface (which the standard
// go.opentelemetry.io/otel/sdk/trace.TracerProvider does).
//
// Pass a custom Flusher when you need to flush more than just trace
// telemetry — for example, to flush a batched MeterProvider or LogProvider
// alongside the TracerProvider:
//
//	app.Use(otelfunc.Middleware(
//	    otelfunc.WithFlusher(otelfunc.MultiFlusher(tracerProvider, meterProvider, loggerProvider)),
//	))
//
// To disable flushing entirely (for example, when running on a plan where
// the worker is never frozen and a synchronous exporter is in use), use
// [WithoutFlusher] instead of constructing a no-op Flusher manually.
func WithFlusher(f Flusher) Option {
	return optionFunc(func(c *config) {
		c.flusher = f
		c.flusherSet = true
	})
}

// WithoutFlusher disables the post-invocation force-flush. Intended for
// callers running on always-warm plans (Premium, Dedicated) with a
// synchronous span processor where flushing is unnecessary, or for unit
// tests that want to assert no flush happens.
//
// Most callers should leave the default Flusher in place — losing
// telemetry to a frozen container is a nasty failure mode that takes
// hours to diagnose, and the cost of an unnecessary ForceFlush against a
// SyncSpanProcessor or empty batch is negligible.
func WithoutFlusher() Option {
	return optionFunc(func(c *config) {
		c.flusher = nil
		c.flusherSet = true
	})
}

// WithSpanNameFormatter overrides the function used to compute the span
// name from the invocation. The default returns ic.FunctionName, matching
// otellambda's behavior of using the function name as the span name.
func WithSpanNameFormatter(fn func(*sdk.InvocationContext) string) Option {
	return optionFunc(func(c *config) {
		if fn != nil {
			c.spanName = fn
		}
	})
}

// WithAttributes adds extra attributes to every span produced by the
// Middleware. Useful for resource-level tags that aren't captured by the
// TracerProvider's Resource — e.g. deployment slot, region overrides.
func WithAttributes(attrs ...attribute.KeyValue) Option {
	return optionFunc(func(c *config) {
		c.extraAttrs = append(c.extraAttrs, attrs...)
	})
}

type config struct {
	tp         trace.TracerProvider
	propagator propagation.TextMapPropagator
	flusher    Flusher
	flusherSet bool // tracks whether the user explicitly set a flusher (or disabled it)
	spanName   func(*sdk.InvocationContext) string
	extraAttrs []attribute.KeyValue
}

// Middleware returns an [sdk.Middleware] that traces every function
// invocation as an OpenTelemetry server-kind span.
//
// On each invocation the Middleware:
//
//  1. Extracts incoming W3C trace context from sdk.InvocationContext.TraceContext
//     and attaches the resulting SpanContext to ctx via the configured
//     propagator. Spans the user starts inside their handler with
//     tracer.Start(ctx, ...) thus correlate with the host's parent span.
//  2. Starts a server-kind span named after the function (override via
//     [WithSpanNameFormatter]) with FaaS attributes:
//     faas.invocation_id, faas.name, faas.trigger.
//  3. Records any error returned by the inner Handler on the span and sets
//     the span status to Error.
//  4. Calls Flusher.ForceFlush before returning, so telemetry is pushed
//     before the host may freeze the worker on consumption-style plans.
//     The default Flusher is the configured TracerProvider when it
//     implements the [Flusher] interface (which the standard
//     go.opentelemetry.io/otel/sdk/trace TracerProvider does); use
//     [WithFlusher] to override or [WithoutFlusher] to disable.
func Middleware(opts ...Option) sdk.Middleware {
	cfg := &config{
		tp:         otel.GetTracerProvider(),
		propagator: otel.GetTextMapPropagator(),
		spanName:   defaultSpanName,
	}
	for _, o := range opts {
		o.apply(cfg)
	}
	// Auto-flush via the TracerProvider when the user hasn't configured a
	// flusher explicitly. This is the safe default on Functions consumption
	// plans where the worker process can be frozen between invocations and
	// any buffered batches in a BatchSpanProcessor would be lost.
	if !cfg.flusherSet {
		if f, ok := cfg.tp.(Flusher); ok {
			cfg.flusher = f
		}
	}
	tracer := cfg.tp.Tracer(ScopeName)

	return sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, ic *sdk.InvocationContext) error {
			ctx = cfg.propagator.Extract(ctx, traceContextCarrier(ic))

			attrs := []attribute.KeyValue{
				semconv.FaaSInvocationID(ic.InvocationID),
				semconv.FaaSName(ic.FunctionName),
				attribute.String("faas.trigger", classifyTrigger(ic.TriggerType)),
			}
			attrs = append(attrs, cfg.extraAttrs...)

			ctx, span := tracer.Start(ctx, cfg.spanName(ic),
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(attrs...),
			)

			err := next(ctx, ic)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			span.End()

			// Force-flush before the worker may be frozen. Done after
			// span.End so the just-completed span is included in the
			// flush batch.
			if cfg.flusher != nil {
				if flushErr := cfg.flusher.ForceFlush(ctx); flushErr != nil && !errors.Is(flushErr, context.Canceled) {
					// Don't fail the invocation on a flush error;
					// telemetry is auxiliary, not essential.
					log.Printf("otelfunc: ForceFlush failed: %v", flushErr)
				}
			}
			return err
		}
	})
}

// defaultSpanName is the default WithSpanNameFormatter — it returns the
// function's user-facing name. Falls back to "azure-functions-invocation"
// when ic carries no name (defensive; should not happen in practice).
func defaultSpanName(ic *sdk.InvocationContext) string {
	if ic == nil || ic.FunctionName == "" {
		return "azure-functions-invocation"
	}
	return ic.FunctionName
}

// traceContextCarrier adapts the incoming sdk.TraceContext to the
// TextMapCarrier shape that propagation.TraceContext expects. Only the W3C
// keys (traceparent, tracestate) are surfaced; the host's Attributes map is
// not part of the standard W3C extraction contract and is left available on
// ic.TraceContext.Attributes for users that want to read it directly.
func traceContextCarrier(ic *sdk.InvocationContext) propagation.MapCarrier {
	mc := propagation.MapCarrier{}
	if ic.TraceContext.TraceParent != "" {
		mc["traceparent"] = ic.TraceContext.TraceParent
	}
	if ic.TraceContext.TraceState != "" {
		mc["tracestate"] = ic.TraceContext.TraceState
	}
	return mc
}

// classifyTrigger maps Azure Functions binding type names to the value set
// defined by the OpenTelemetry FaaS semantic conventions for the
// faas.trigger attribute.
//
// See https://opentelemetry.io/docs/specs/semconv/faas/faas-spans/#faas-trigger
// Allowed values: "datasource", "http", "pubsub", "timer", "other".
func classifyTrigger(t string) string {
	switch t {
	case "httpTrigger":
		return "http"
	case "timerTrigger":
		return "timer"
	case "blobTrigger", "cosmosDBTrigger":
		return "datasource"
	case "eventHubTrigger", "serviceBusTrigger",
		"serviceBusQueueTrigger", "serviceBusTopicTrigger",
		"eventGridTrigger":
		return "pubsub"
	default:
		return "other"
	}
}
