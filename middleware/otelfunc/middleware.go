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
//	    app := sdk.FunctionApp()
//	    app.Use(otelfunc.Middleware(otelfunc.WithExporter(myExporter)))
//
//	    app.HTTP("hello", helloHandler)
//	    worker.Start(app)
//	}
//
// The middleware honors three setup paths in priority order:
//
//  1. [WithTracerProvider] — caller hands us a TracerProvider. We use it as-is.
//  2. [WithExporter] — caller hands us a SpanExporter. We build a TracerProvider
//     around it with a default Resource carrying cloud.provider=azure /
//     cloud.platform=azure_functions / service.name (from WEBSITE_SITE_NAME or
//     OTEL_SERVICE_NAME).
//  3. Otherwise — we fall back to otel.GetTracerProvider().
//
// Capability advertising:
//
// When a non-noop TracerProvider is in play and the
// AZURE_FUNCTIONS_WORKER_OPENTELEMETRY_DISABLED env var is not truthy, the
// middleware reports the worker-level capabilities WorkerOpenTelemetryEnabled
// and WorkerOpenTelemetrySchemaVersion via the [sdk.CapabilityProvider]
// contract. The worker copies those flags into WorkerInitResponse.Capabilities
// so the host knows the worker is emitting OpenTelemetry telemetry directly
// and should not double-emit to Application Insights for the same invocation.
//
// Force-flushing:
//
// On consumption-style plans (Flex Consumption, Linux Consumption) the host
// may freeze the worker process between invocations and lose any telemetry
// the exporter has buffered. The middleware therefore force-flushes after
// every invocation when the configured TracerProvider implements [Flusher]
// (which the standard go.opentelemetry.io/otel/sdk/trace TracerProvider
// does). Override with [WithFlusher] or disable with [WithoutFlusher].
//
// Design follows aws-lambda-go's otellambda package: a thin Middleware
// that (a) extracts the host's W3C trace context from
// sdk.InvocationContext, (b) starts a server-kind span around the user
// handler, and (c) force-flushes telemetry after each invocation.
package otelfunc

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"

	"github.com/azure/azure-functions-golang-worker/sdk"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
)

// ScopeName is the OpenTelemetry instrumentation scope used by the
// Middleware when obtaining a Tracer from the configured TracerProvider.
const ScopeName = "github.com/azure/azure-functions-golang-worker/middleware/otelfunc"

// SchemaVersion is the OpenTelemetry semantic conventions schema version
// the middleware advertises to the host through the
// WorkerOpenTelemetrySchemaVersion capability. It must match the version
// of the imported semconv package — currently v1.27.0.
const SchemaVersion = "1.27.0"

// CapabilityWorkerOpenTelemetryEnabled is the worker-level capability key
// the middleware advertises when an active (non-noop) TracerProvider is
// wired up. The host uses it to decide whether to skip its own
// Application Insights emission for invocations served by this worker.
const CapabilityWorkerOpenTelemetryEnabled = "WorkerOpenTelemetryEnabled"

// CapabilityWorkerOpenTelemetrySchemaVersion is the worker-level capability
// key reporting the semantic conventions schema version the worker is
// emitting. Paired with [CapabilityWorkerOpenTelemetryEnabled].
const CapabilityWorkerOpenTelemetrySchemaVersion = "WorkerOpenTelemetrySchemaVersion"

// EnvDisable is the environment variable that, when set to a truthy value
// (true, 1, yes), disables the OpenTelemetry middleware entirely. The
// middleware becomes a pass-through and advertises no capability.
//
// Use case: ops kill switch without redeploying the app, or selective
// disabling on a single slot for triage.
const EnvDisable = "AZURE_FUNCTIONS_WORKER_OPENTELEMETRY_DISABLED"

// EnvServiceName is the OpenTelemetry-standard environment variable for
// overriding service.name on the default Resource the middleware builds
// when constructing a TracerProvider via [WithExporter]. If unset, falls
// back to WEBSITE_SITE_NAME (the App Service / Functions site name) and
// finally to "azure-functions".
const EnvServiceName = "OTEL_SERVICE_NAME"

// envWebsiteSiteName is the Functions/App Service environment variable
// carrying the site name; we use it as a fallback for service.name.
const envWebsiteSiteName = "WEBSITE_SITE_NAME"

// Flusher is the optional contract the Middleware uses to push pending
// telemetry to the configured exporter at the end of each invocation.
//
// The standard go.opentelemetry.io/otel/sdk/trace.TracerProvider satisfies
// Flusher via its ForceFlush method, so by default the Middleware uses
// the configured TracerProvider as the flusher (see [Middleware]).
//
// Force-flushing is essential on consumption-style plans (Flex
// Consumption, Linux Consumption) where the host may freeze the process
// between invocations and lose buffered batches.
type Flusher interface {
	ForceFlush(ctx context.Context) error
}

// Option configures the Middleware. Pass options into Middleware to
// customize the TracerProvider, exporter, propagator, flusher, span
// name, or extra attributes.
type Option interface {
	apply(*config)
}

type optionFunc func(*config)

func (f optionFunc) apply(c *config) { f(c) }

// WithTracerProvider sets the OpenTelemetry TracerProvider used to obtain
// the Tracer. Highest priority — wins over [WithExporter] and
// otel.GetTracerProvider().
func WithTracerProvider(tp trace.TracerProvider) Option {
	return optionFunc(func(c *config) {
		if tp != nil {
			c.tp = tp
		}
	})
}

// WithExporter wires up a SpanExporter. The middleware will build a
// TracerProvider around the exporter using a [BatchSpanProcessor] and a
// default Resource carrying cloud.provider=azure,
// cloud.platform=azure_functions, and a derived service.name (from
// OTEL_SERVICE_NAME, then WEBSITE_SITE_NAME, then "azure-functions").
//
// Use this when you want sane defaults without constructing a
// TracerProvider yourself; for full control, use [WithTracerProvider].
//
// Ignored when [WithTracerProvider] is also passed.
func WithExporter(e sdktrace.SpanExporter) Option {
	return optionFunc(func(c *config) {
		c.exporter = e
	})
}

// WithPropagator sets the TextMapPropagator used to extract incoming W3C
// trace context from sdk.InvocationContext. Defaults to the global
// otel.GetTextMapPropagator() if non-empty, otherwise propagation.TraceContext{}.
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
	exporter   sdktrace.SpanExporter
	propagator propagation.TextMapPropagator
	flusher    Flusher
	flusherSet bool // tracks whether the user explicitly set a flusher (or disabled it)
	spanName   func(*sdk.InvocationContext) string
	extraAttrs []attribute.KeyValue
}

// otelMiddleware is the [sdk.Middleware] implementation Middleware
// returns. It also satisfies [sdk.CapabilityProvider] so the worker
// dispatcher can pick up the OpenTelemetry capability flags at App.Use
// time.
type otelMiddleware struct {
	tracer       trace.Tracer
	cfg          *config
	enabled      bool
	capabilities map[string]string
}

// Wrap implements [sdk.Middleware].
func (m *otelMiddleware) Wrap(next sdk.Handler) sdk.Handler {
	if !m.enabled {
		// Pass-through: env disable or noop TP. User code that calls
		// otel.Tracer(...).Start(...) on its own still works; we just
		// stay out of the way.
		return next
	}
	return func(ctx context.Context, ic *sdk.InvocationContext) error {
		ctx = m.cfg.propagator.Extract(ctx, traceContextCarrier(ic))

		// Inbound baggage: hydrate ctx with the host-supplied baggage
		// map so user code reading baggage.FromContext(ctx) sees what
		// upstream services attached. The user can then propagate this
		// baggage to their own downstream calls by using the standard
		// otelhttp / otelgrpc instrumentations, which read baggage off
		// ctx at call time.
		if inboundBag := buildInboundBaggage(ic.TraceContext.Baggage); inboundBag.Len() > 0 {
			ctx = baggage.ContextWithBaggage(ctx, inboundBag)
		}

		attrs := []attribute.KeyValue{
			semconv.FaaSInvocationID(ic.InvocationID),
			semconv.FaaSName(ic.FunctionName),
			attribute.String("faas.trigger", classifyTrigger(ic.TriggerType)),
		}
		attrs = append(attrs, m.cfg.extraAttrs...)

		ctx, span := m.tracer.Start(ctx, m.cfg.spanName(ic),
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
		if m.cfg.flusher != nil {
			if flushErr := m.cfg.flusher.ForceFlush(ctx); flushErr != nil && !errors.Is(flushErr, context.Canceled) {
				slog.WarnContext(ctx, "otelfunc: ForceFlush failed", "err", flushErr)
			}
		}
		return err
	}
}

// Capabilities implements [sdk.CapabilityProvider]. Returns the OTel
// capability flags only when the middleware is active (see [Middleware]).
func (m *otelMiddleware) Capabilities() map[string]string {
	out := make(map[string]string, len(m.capabilities))
	for k, v := range m.capabilities {
		out[k] = v
	}
	return out
}

// Middleware returns an [sdk.Middleware] that traces every function
// invocation as an OpenTelemetry server-kind span. The returned value also
// implements [sdk.CapabilityProvider]; the worker dispatcher reads the
// capability map at App.Use time and forwards it to the host via
// WorkerInitResponse.Capabilities.
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
//
// The middleware becomes a pass-through (no spans, no capability
// advertising) when:
//
//   - The AZURE_FUNCTIONS_WORKER_OPENTELEMETRY_DISABLED env var is set
//     to a truthy value, or
//   - The configured TracerProvider is the OpenTelemetry noop (i.e. no
//     real exporter is wired up).
//
// In pass-through mode, user-side OpenTelemetry calls still work — the
// middleware just does not contribute spans of its own.
func Middleware(opts ...Option) sdk.Middleware {
	cfg := &config{
		spanName: defaultSpanName,
	}
	for _, o := range opts {
		o.apply(cfg)
	}
	if cfg.propagator == nil {
		cfg.propagator = defaultPropagator()
	}

	m := &otelMiddleware{cfg: cfg}

	if isDisabledByEnv() {
		// Pass-through: user explicitly disabled OTel via env var.
		return m
	}

	// Resolve TracerProvider in priority order:
	//   1. WithTracerProvider — explicitly given, use as-is.
	//   2. WithExporter — build a TracerProvider with a default Resource.
	//   3. otel.GetTracerProvider() — honor the global, may be a noop.
	//
	// We only run the (relatively expensive) noop detection in case 3,
	// where we cannot otherwise tell whether the global has been wired
	// up. If the user explicitly handed us a TracerProvider or exporter,
	// we trust their configuration and advertise the capability.
	usedFallback := false
	if cfg.tp == nil && cfg.exporter != nil {
		cfg.tp = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(cfg.exporter),
			sdktrace.WithResource(buildDefaultResource()),
		)
	}
	if cfg.tp == nil {
		cfg.tp = otel.GetTracerProvider()
		usedFallback = true
	}

	if usedFallback && isNoopTracerProvider(cfg.tp) {
		// Pass-through: no real exporter is wired up. The user has not
		// asked for OpenTelemetry, so we should not advertise the
		// capability or override the host's telemetry path.
		return m
	}

	// Auto-flush via the TracerProvider when the user hasn't configured a
	// flusher explicitly. The standard sdk/trace.TracerProvider satisfies
	// Flusher; user-supplied custom TPs may not.
	if !cfg.flusherSet {
		if f, ok := cfg.tp.(Flusher); ok {
			cfg.flusher = f
		}
	}

	m.tracer = cfg.tp.Tracer(ScopeName)
	m.enabled = true
	m.capabilities = map[string]string{
		CapabilityWorkerOpenTelemetryEnabled:       "true",
		CapabilityWorkerOpenTelemetrySchemaVersion: SchemaVersion,
	}
	return m
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

// defaultPropagator returns the global propagator if configured, otherwise
// the standard W3C TraceContext propagator. The Functions host emits W3C
// trace context, so this is the right default.
func defaultPropagator() propagation.TextMapPropagator {
	p := otel.GetTextMapPropagator()
	if p == nil {
		return propagation.TraceContext{}
	}
	return p
}

// traceContextCarrier adapts the incoming sdk.TraceContext to the
// TextMapCarrier shape that propagation.TraceContext expects. Only the
// W3C keys (traceparent, tracestate) are surfaced here; baggage handling
// lives in commit 5.
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

// buildDefaultResource constructs the OpenTelemetry Resource used when
// the middleware builds its own TracerProvider via [WithExporter]. The
// Resource carries:
//
//   - cloud.provider = azure
//   - cloud.platform = azure_functions
//   - service.name   = OTEL_SERVICE_NAME / WEBSITE_SITE_NAME / "azure-functions"
//
// resource.Default() already incorporates the standard environment
// detector (OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES) so callers can
// override any of the above with the standard OTel env vars.
func buildDefaultResource() *resource.Resource {
	attrs := []attribute.KeyValue{
		semconv.CloudProviderAzure,
		semconv.CloudPlatformAzureFunctions,
	}
	if name := serviceNameFromEnv(); name != "" {
		attrs = append(attrs, semconv.ServiceName(name))
	}

	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, attrs...),
	)
	if err != nil {
		// resource.Merge fails only on schema URL mismatch; build a fresh
		// Resource as a fallback so we still produce something usable.
		return resource.NewWithAttributes(semconv.SchemaURL, attrs...)
	}
	return r
}

// serviceNameFromEnv resolves the service.name fallback chain. Returns
// "" when nothing is set, so the caller can decide whether to add the
// attribute at all (resource.Default already supplies a generic
// "unknown_service:..." value).
func serviceNameFromEnv() string {
	if v := strings.TrimSpace(os.Getenv(EnvServiceName)); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv(envWebsiteSiteName)); v != "" {
		return v
	}
	return ""
}

// isDisabledByEnv reports whether the AZURE_FUNCTIONS_WORKER_OPENTELEMETRY_DISABLED
// env var is set to a truthy value.
func isDisabledByEnv() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(EnvDisable)))
	switch v {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// isNoopTracerProvider reports whether the given TracerProvider produces
// non-recording spans, which is the OpenTelemetry-API-level signal that
// no real exporter is wired up.
//
// The detection is behavioral: we ask the TracerProvider for a Tracer,
// start a span, and check span.IsRecording. The OpenTelemetry global
// default returns a delegating TracerProvider that points at a noop
// internally — type-name detection of "noop" doesn't catch it, but
// IsRecording on a span from such a provider returns false.
//
// A user who wires up a real TracerProvider but configures an
// always-reject sampler would also be detected as noop here, and the
// middleware would not advertise the capability. That is the correct
// outcome: if no spans are ever recorded, there is no telemetry for the
// host to defer on, so leaving the host's Application Insights emission
// in place is the safer default.
func isNoopTracerProvider(tp trace.TracerProvider) bool {
	if tp == nil {
		return true
	}
	tracer := tp.Tracer("otelfunc/noop-detect")
	_, span := tracer.Start(context.Background(), "noop-detect")
	defer span.End()
	return !span.IsRecording()
}

// buildInboundBaggage converts the host-supplied baggage map into an
// OpenTelemetry baggage.Baggage so user code can read it via the
// standard baggage.FromContext(ctx) entry point. Invalid entries (per
// the W3C baggage spec) are skipped silently — the inbound channel is
// trusted host data, but defending against malformed values keeps the
// middleware robust to upstream changes.
func buildInboundBaggage(in map[string]string) baggage.Baggage {
	bag := baggage.Baggage{}
	if len(in) == 0 {
		return bag
	}
	for k, v := range in {
		member, err := baggage.NewMemberRaw(k, v)
		if err != nil {
			continue
		}
		next, err := bag.SetMember(member)
		if err != nil {
			continue
		}
		bag = next
	}
	return bag
}
