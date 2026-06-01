// Package otelfunc provides an OpenTelemetry-tracing middleware for the
// azure-functions-golang-worker SDK.
//
// Apps that want distributed tracing import this package and register the
// Middleware via App.Use. The simplest setup is the zero-arg form combined
// with the standard OTel env vars on the Function App:
//
//	import (
//	    "github.com/azure/azure-functions-golang-worker/sdk"
//	    "github.com/azure/azure-functions-golang-worker/middleware/otelfunc"
//	)
//
//	func main() {
//	    app := sdk.FunctionApp()
//	    app.Use(otelfunc.Middleware())
//
//	    app.HTTP("hello", helloHandler)
//	    worker.Start(app)
//	}
//
// With OTEL_EXPORTER_OTLP_ENDPOINT (and optionally
// OTEL_EXPORTER_OTLP_HEADERS, OTEL_SERVICE_NAME) set in the app settings,
// the middleware auto-configures both a TracerProvider and a
// LoggerProvider against the OTLP HTTP endpoint, wires force-flush, and
// registers a clean shutdown — no in-code provider plumbing required.
//
// The middleware honors four setup paths in priority order:
//
//  1. [WithTracerProvider] — caller hands us a TracerProvider. We use it
//     as-is. (Same shape for [WithLoggerProvider].)
//  2. [WithExporter] — caller hands us one or more SpanExporters. We build
//     a TracerProvider around them with a default Resource carrying
//     cloud.provider=azure / cloud.platform=azure_functions / service.name
//     (from OTEL_SERVICE_NAME or WEBSITE_SITE_NAME). (Same shape for
//     [WithLogExporter].)
//  3. OTEL_EXPORTER_OTLP_ENDPOINT env var — auto-build an OTLP HTTP
//     TracerProvider and LoggerProvider. This is preferred over the
//     OTel global because the global is a delegating wrapper that
//     reports as non-noop even when nothing is wired up.
//  4. otel.GetTracerProvider() / global.GetLoggerProvider() — only when
//     the global is a true non-wrapper non-noop provider.
//
// Use [WithResource] to extend the default Resource with deployment
// attributes (service.version, deployment.environment, build SHA, etc.)
// that you'd rather supply in code than via OTEL_RESOURCE_ATTRIBUTES.
//
// Capability advertising:
//
// When a non-noop TracerProvider is in play and the
// AZURE_FUNCTIONS_WORKER_OPENTELEMETRY_DISABLED env var is not truthy, the
// middleware reports the worker-level capabilities WorkerOpenTelemetryEnabled
// and WorkerOpenTelemetrySchemaVersion via the [sdk.CapabilityProvider]
// contract. The worker copies those flags into WorkerInitResponse.Capabilities
// so the host knows the worker is emitting OpenTelemetry telemetry directly.
// The host honors WorkerOpenTelemetryEnabled narrowly: it suppresses the
// forwarding of worker-emitted `Function.*` log records into its own
// OpenTelemetry log pipeline. Host-emitted spans (AspNetCore HTTP server,
// Functions.Host init activity) keep flowing because they describe the
// host's own activity, not the worker's.
//
// Force-flushing:
//
// On consumption-style plans (Flex Consumption, Linux Consumption) the host
// may freeze the worker process between invocations and lose any telemetry
// the exporter has buffered. The middleware therefore force-flushes after
// every invocation when the configured TracerProvider implements [Flusher]
// (which the standard go.opentelemetry.io/otel/sdk/trace TracerProvider
// does). The owned LoggerProvider is also force-flushed on every
// invocation. Override the TracerProvider flusher with [WithFlusher] or
// disable with [WithoutFlusher].
//
// Graceful shutdown:
//
// When the middleware constructs its own TracerProvider or LoggerProvider
// (via [WithExporter] / [WithLogExporter] / auto-OTLP), it also implements
// [sdk.ShutdownProvider]. The worker invokes Shutdown after the gRPC
// stream closes or on SIGTERM/SIGINT, so user code does not need a
// defer cleanup() line in main(). User-supplied providers (passed via
// [WithTracerProvider] / [WithLoggerProvider]) are NOT shut down — they
// remain the user's lifecycle to manage.
package otelfunc

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker/log"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	olog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	lognoop "go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

// ScopeName is the OpenTelemetry instrumentation scope used by the
// Middleware when obtaining a Tracer from the configured TracerProvider.
const ScopeName = "github.com/azure/azure-functions-golang-worker/middleware/otelfunc"

// SchemaVersion is the OpenTelemetry semantic conventions schema version
// the middleware advertises to the host through the
// WorkerOpenTelemetrySchemaVersion capability. It must match the version
// of the imported semconv package — currently v1.37.0. This matches the
// dotnet-isolated worker's default schema version, so the host's known-
// attribute filter list (keyed on the advertised schema version) lines
// up across runtimes.
const SchemaVersion = "1.37.0"

// CapabilityWorkerOpenTelemetryEnabled is the worker-level capability key
// the middleware advertises when an active (non-noop) TracerProvider is
// wired up. The host uses it to suppress forwarding of worker-emitted
// `Function.*` log records into the host's own OpenTelemetry log
// pipeline -- the worker is expected to emit those itself via its own
// LoggerProvider. Host-emitted spans (AspNetCore HTTP server,
// Functions.Host init activity) are unaffected by this capability and
// continue to flow through the host's telemetry pipeline.
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

// Azure Functions / App Service environment variables used to populate
// OpenTelemetry Resource attributes that match what the Java worker
// emits, so cross-runtime dashboards filter on the same keys.
const (
	envRegionName           = "REGION_NAME"            // e.g. "eastus2"
	envWebsiteResourceGroup = "WEBSITE_RESOURCE_GROUP" // e.g. "my-rg"
	envWebsiteOwnerName     = "WEBSITE_OWNER_NAME"     // "<subscriptionId>+<stamp>..."
	envWebsiteSlotName      = "WEBSITE_SLOT_NAME"      // "production" / "staging" / etc.
)

// Inbound RpcTraceContext.Attributes keys the host sends on every
// InvocationRequest. We promote a subset onto the per-invocation span
// for diagnostic correlation, matching what the Java worker does.
const (
	hostAttrProcessID         = "ProcessId"
	hostAttrHostInstanceID    = "HostInstanceId"
	hostAttrLiveLogsSessionID = "#AzFuncLiveLogsSessionId"
)

// Flusher is the optional contract the Middleware uses to push pending
// telemetry to the configured exporter at the end of each invocation.
//
// The standard go.opentelemetry.io/otel/sdk/trace.TracerProvider satisfies
// Flusher via its ForceFlush method, so by default the Middleware uses
// the configured TracerProvider as the flusher (see [Middleware]).
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
// May be called multiple times to fan out spans to multiple backends
// (e.g. one OTLP exporter pointing at New Relic, another at a custom
// collector). Each exporter is wrapped in its own BatchSpanProcessor on
// the same TracerProvider, so every span is delivered to all of them.
//
// Use this when you want sane defaults without constructing a
// TracerProvider yourself; for full control over Resource, sampler, or
// processor types, use [WithTracerProvider].
//
// Ignored when [WithTracerProvider] is also passed.
func WithExporter(e sdktrace.SpanExporter) Option {
	return optionFunc(func(c *config) {
		if e == nil {
			return
		}
		c.exporters = append(c.exporters, e)
	})
}

// WithLoggerProvider sets the OpenTelemetry LoggerProvider the SDK's
// slog handler will bridge user log records to. Highest priority — wins
// over [WithLogExporter] and the global [log.LoggerProvider].
//
// When a non-noop LoggerProvider is configured (whether explicitly via
// this option, via [WithLogExporter], via the global, or via the
// OTEL_EXPORTER_OTLP_ENDPOINT auto-config) the middleware sets it as the
// global LoggerProvider so the SDK's slog→OTel bridge picks it up.
func WithLoggerProvider(lp olog.LoggerProvider) Option {
	return optionFunc(func(c *config) {
		if lp != nil {
			c.lp = lp
		}
	})
}

// WithLogExporter wires up a log Exporter. The middleware will build a
// LoggerProvider around the exporter using a [BatchProcessor] and the
// same default Resource as [WithExporter], then set it as the global
// LoggerProvider so user slog records flow through the OTel logs
// pipeline alongside the host's RpcLog channel.
//
// May be called multiple times to fan out log records to multiple
// backends. Each exporter is wrapped in its own BatchProcessor on the
// same LoggerProvider, so every record is delivered to all of them.
//
// Use this when you want sane defaults without constructing a
// LoggerProvider yourself; for full control over Resource or processor
// types, use [WithLoggerProvider].
//
// Ignored when [WithLoggerProvider] is also passed.
func WithLogExporter(e sdklog.Exporter) Option {
	return optionFunc(func(c *config) {
		if e == nil {
			return
		}
		c.logExporters = append(c.logExporters, e)
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
// name from the invocation. The default returns "function <FunctionName>",
// matching the dotnet-isolated and Java workers.
func WithSpanNameFormatter(fn func(*sdk.InvocationContext) string) Option {
	return optionFunc(func(c *config) {
		if fn != nil {
			c.spanName = fn
		}
	})
}

// WithAttributes adds extra attributes to every span produced by the
// Middleware. Use this for span-level annotations (e.g. deployment slot,
// region override). For Resource-level attributes, use [WithResource].
func WithAttributes(attrs ...attribute.KeyValue) Option {
	return optionFunc(func(c *config) {
		c.extraAttrs = append(c.extraAttrs, attrs...)
	})
}

// WithResource appends caller-supplied attributes to the default
// Resource the middleware uses when building its own TracerProvider /
// LoggerProvider (via [WithExporter], [WithLogExporter], or env-var
// auto-OTLP). The attributes are merged on top of the default Resource
// (cloud.provider=azure, cloud.platform=azure_functions, service.name)
// so callers can extend rather than replace the defaults.
//
// Common use: stamp every span and log record with deployment-scoped
// metadata that isn't suitable for OTEL_RESOURCE_ATTRIBUTES (e.g. a
// build SHA injected via -ldflags, a typed numeric value, or anything
// you'd rather not configure as a string in app settings):
//
//	app.Use(otelfunc.Middleware(
//	    otelfunc.WithResource(
//	        semconv.ServiceVersion(buildVersion),
//	        semconv.DeploymentEnvironmentName("production"),
//	        attribute.String("build.sha", buildSHA),
//	    ),
//	))
//
// May be called multiple times; attributes accumulate. Later attributes
// override earlier ones with the same key (matching resource.Merge
// semantics).
//
// Has no effect when [WithTracerProvider] AND [WithLoggerProvider] are
// both set, since neither owned provider is built in that case. When
// only one is set, the Resource attributes still apply to the other.
func WithResource(attrs ...attribute.KeyValue) Option {
	return optionFunc(func(c *config) {
		c.resourceAttrs = append(c.resourceAttrs, attrs...)
	})
}

type config struct {
	tp            trace.TracerProvider
	exporters     []sdktrace.SpanExporter
	lp            olog.LoggerProvider
	logExporters  []sdklog.Exporter
	propagator    propagation.TextMapPropagator
	flusher       Flusher
	flusherSet    bool // tracks whether the user explicitly set a flusher (or disabled it)
	spanName      func(*sdk.InvocationContext) string
	extraAttrs    []attribute.KeyValue
	resourceAttrs []attribute.KeyValue // appended to default Resource when middleware builds the providers
}

// otelMiddleware is the [sdk.Middleware] implementation Middleware
// returns. It also satisfies [sdk.CapabilityProvider] so the worker
// dispatcher can pick up the OpenTelemetry capability flags at App.Use
// time, and [sdk.ShutdownProvider] so any TracerProvider/LoggerProvider
// the middleware built itself gets flushed and released cleanly when the
// worker exits.
type otelMiddleware struct {
	tracer       trace.Tracer
	cfg          *config
	enabled      bool
	capabilities map[string]string

	// ownedTP / ownedLP are set when Middleware constructed the providers
	// itself (via WithExporter, WithLogExporter, or env-var auto-config).
	// Only owned providers are shut down by [otelMiddleware.Shutdown];
	// user-supplied providers are the user's responsibility.
	ownedTP *sdktrace.TracerProvider
	ownedLP *sdklog.LoggerProvider
}

// Wrap implements [sdk.Middleware].
func (m *otelMiddleware) Wrap(next sdk.Handler) sdk.Handler {
	if !m.enabled {
		// Pass-through: env disable or noop TP. User code that calls
		// otel.Tracer(...).Start(...) on its own still works; we just
		// stay out of the way.
		return next
	}
	return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
		ctx = m.cfg.propagator.Extract(ctx, traceContextCarrier(mc.InvocationContext))

		// Inbound baggage: hydrate ctx with the host-supplied baggage map
		// so user code reading baggage.FromContext(ctx) sees what upstream
		// services attached.
		if inboundBag := buildInboundBaggage(mc.TraceContext.Baggage); inboundBag.Len() > 0 {
			ctx = baggage.ContextWithBaggage(ctx, inboundBag)
		}

		attrs := []attribute.KeyValue{
			semconv.FaaSInvocationID(mc.InvocationID),
			semconv.FaaSName(mc.FunctionName),
			attribute.String("faas.trigger", classifyTrigger(mc.TriggerType)),
		}
		// Promote select inbound RpcTraceContext attributes onto the
		// per-invocation span. These keys match what the .NET host
		// emits and what the Java worker surfaces, so cross-runtime
		// dashboards filter on the same names.
		if hostAttrs := mc.TraceContext.Attributes; len(hostAttrs) > 0 {
			if v := hostAttrs[hostAttrProcessID]; v != "" {
				if pid, err := strconv.Atoi(v); err == nil {
					attrs = append(attrs, semconv.ProcessPID(pid))
				}
			}
			if v := hostAttrs[hostAttrHostInstanceID]; v != "" {
				attrs = append(attrs, semconv.FaaSInstance(v))
			}
			if v := hostAttrs[hostAttrLiveLogsSessionID]; v != "" {
				// The azure.functions.* prefix marks this as an Azure-
				// Functions-specific extension; used by the portal
				// live-log feature to correlate stream sessions with
				// telemetry.
				attrs = append(attrs, attribute.String("azure.functions.live_logs_session_id", v))
			}
		}
		attrs = append(attrs, m.cfg.extraAttrs...)

		ctx, span := m.tracer.Start(ctx, m.cfg.spanName(mc.InvocationContext),
			// SpanKindInternal: the host's Microsoft.AspNetCore
			// instrumentation already owns the SERVER-kind span for
			// HTTP triggers, and non-HTTP triggers don't represent a
			// network server. Matches the Java worker which uses
			// INTERNAL uniformly.
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithAttributes(attrs...),
		)

		err := next(ctx, mc)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		harvestSpanAttributesToOutbound(mc, span)
		span.End()

		// Force-flush before the worker may be frozen. Done after
		// span.End so the just-completed span is included in the
		// flush batch.
		if m.cfg.flusher != nil {
			if flushErr := m.cfg.flusher.ForceFlush(ctx); flushErr != nil && !errors.Is(flushErr, context.Canceled) {
				slog.LogAttrs(ctx, slog.LevelWarn, "otelfunc: ForceFlush failed",
					slog.Any("err", flushErr),
				)
			}
		}
		// Force-flush the LoggerProvider too so log records emitted
		// during the invocation are pushed to the OTel backend before
		// the worker may be frozen between invocations on consumption-
		// style plans. Only owned LPs are flushed -- user-supplied LPs
		// remain the user's lifecycle to manage.
		if m.ownedLP != nil {
			if flushErr := m.ownedLP.ForceFlush(ctx); flushErr != nil && !errors.Is(flushErr, context.Canceled) {
				slog.LogAttrs(ctx, slog.LevelWarn, "otelfunc: LoggerProvider ForceFlush failed",
					slog.Any("err", flushErr),
				)
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
// invocation as an OpenTelemetry internal-kind span named
// "function <FunctionName>" (matching the dotnet-isolated and Java
// workers). The returned value also implements [sdk.CapabilityProvider];
// the worker dispatcher reads the capability map at App.Use time and
// forwards it to the host via WorkerInitResponse.Capabilities.
//
// On each invocation the Middleware extracts the host's inbound W3C
// trace context, hydrates baggage onto ctx, starts the worker invocation
// span, runs the inner Handler, records any error, harvests user-set
// span attributes onto the [sdk.MiddlewareContext] for the host
// (see harvest.go), and force-flushes telemetry before returning.
//
// The middleware becomes a pass-through (no spans, no capability
// advertising) when:
//
//   - The AZURE_FUNCTIONS_WORKER_OPENTELEMETRY_DISABLED env var is set
//     to a truthy value, or
//   - No explicit TracerProvider/exporter was supplied, OTEL_EXPORTER_OTLP_*
//     env vars are not set, AND the OTel global TracerProvider is the
//     noop default. In other words: nothing connected on any of the
//     four resolution paths.
//
// A user who explicitly passes a noop TracerProvider via [WithTracerProvider]
// is honored (capabilities are still advertised) — that is treated as an
// intentional configuration choice, not "unconfigured".
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
	//   3. OTEL_EXPORTER_OTLP_ENDPOINT env var — auto-build an OTLP HTTP
	//      TracerProvider so a vanilla `app.Use(otelfunc.Middleware())`
	//      configures end-to-end tracing with no in-code wiring.
	//   4. otel.GetTracerProvider() — honor a non-noop global.
	//
	// Auto-OTLP is preferred over the global because the OTel global
	// TracerProvider is a delegating wrapper that routes spans through
	// go.opentelemetry.io/auto/sdk — which is pulled in transitively by
	// otelslog and other contrib bridges. The auto SDK reports spans as
	// IsRecording=true so an eBPF agent can capture them later, which
	// makes a behavioral noop check return false even when nothing is
	// actually exporting. If we treat that wrapper as "user configured"
	// we silently swallow every span. Auto-OTLP wins so the env var
	// alone is sufficient configuration; this also matches the
	// LoggerProvider resolution path below.
	tpFromGlobal := false
	if cfg.tp == nil && len(cfg.exporters) > 0 {
		opts := make([]sdktrace.TracerProviderOption, 0, len(cfg.exporters)+1)
		for _, e := range cfg.exporters {
			opts = append(opts, sdktrace.WithBatcher(e))
		}
		opts = append(opts, sdktrace.WithResource(buildDefaultResource(cfg.resourceAttrs...)))
		owned := sdktrace.NewTracerProvider(opts...)
		cfg.tp = owned
		m.ownedTP = owned
	}
	if cfg.tp == nil {
		if owned, err := buildOTLPTracerProvider(cfg.resourceAttrs...); err == nil && owned != nil {
			cfg.tp = owned
			m.ownedTP = owned
		}
	}
	if cfg.tp == nil {
		cfg.tp = otel.GetTracerProvider()
		tpFromGlobal = true
	}

	if tpFromGlobal && isNoopTracerProvider(cfg.tp) {
		// Pass-through: no real exporter is wired up. The user has not
		// asked for OpenTelemetry, so we should not advertise the
		// capability or override the host's telemetry path.
		return m
	}
	if m.ownedTP != nil {
		// Publish the owned TP as the global so user code calling
		// otel.GetTracerProvider() / otel.Tracer(...) for ad-hoc spans
		// inside their handler reaches the same exporter the middleware
		// is using. We only do this for providers we constructed --
		// when the user explicitly passed WithTracerProvider they keep
		// full control of global state (matches OTel convention that
		// libraries do not mutate globals out from under callers).
		otel.SetTracerProvider(cfg.tp)
	}

	// Resolve LoggerProvider in priority order:
	//   1. WithLoggerProvider — explicitly given, use as-is.
	//   2. WithLogExporter — build a LoggerProvider with a default Resource.
	//   3. OTEL_EXPORTER_OTLP_ENDPOINT — auto-build an OTLP HTTP LoggerProvider.
	//   4. global.GetLoggerProvider() — honor a user-installed global.
	//
	// Auto-OTLP is preferred over the global because the OTel global
	// LoggerProvider is a delegating wrapper that always reports as
	// non-noop via type assertion; if we treat the wrapper as "user
	// configured", we'd skip auto-OTLP even when the user only set the
	// OTEL_EXPORTER_OTLP_ENDPOINT env var. Auto-OTLP wins so the env
	// var alone is sufficient configuration.
	lpFromGlobal := false
	if cfg.lp == nil && len(cfg.logExporters) > 0 {
		opts := make([]sdklog.LoggerProviderOption, 0, len(cfg.logExporters)+1)
		for _, e := range cfg.logExporters {
			opts = append(opts, sdklog.WithProcessor(sdklog.NewBatchProcessor(e)))
		}
		opts = append(opts, sdklog.WithResource(buildDefaultResource(cfg.resourceAttrs...)))
		ownedLP := sdklog.NewLoggerProvider(opts...)
		cfg.lp = ownedLP
		m.ownedLP = ownedLP
	}
	if cfg.lp == nil {
		// Auto-OTLP for logs mirrors the trace path. Builds only when
		// OTEL_EXPORTER_OTLP_ENDPOINT (or per-signal override) is set.
		if owned, err := buildOTLPLoggerProvider(cfg.resourceAttrs...); err == nil && owned != nil {
			cfg.lp = owned
			m.ownedLP = owned
		}
	}
	if cfg.lp == nil {
		if existing := global.GetLoggerProvider(); !isNoopLoggerProvider(existing) {
			cfg.lp = existing
			lpFromGlobal = true
		}
	}
	if cfg.lp != nil && !lpFromGlobal {
		// Only set the global when we have a NEW provider; otherwise
		// we'd be self-delegating the global wrapper to itself, which
		// the OTel global package warns about and which is a no-op.
		global.SetLoggerProvider(cfg.lp)
	}

	// Register a one-time log.Observer that bridges every user
	// slog record into the global OTel LoggerProvider. We do this here
	// rather than in worker/system_logger.go so users who don't import
	// otelfunc never pay the binary-size cost of the otelslog bridge or
	// the OTel log SDK. registerOTelLogObserverOnce ensures the observer
	// is registered exactly once per process even if Middleware is
	// constructed multiple times (e.g. tests).
	if cfg.lp != nil {
		registerOTelLogObserverOnce()
	}

	// Auto-flush via the TracerProvider when the user hasn't configured a
	// flusher explicitly. The standard sdk/trace.TracerProvider satisfies
	// Flusher; user-supplied custom TPs may not.
	if !cfg.flusherSet {
		if f, ok := cfg.tp.(Flusher); ok {
			cfg.flusher = f
		}
	}

	m.tracer = cfg.tp.Tracer(ScopeName,
		trace.WithInstrumentationVersion(resolveSDKVersion()),
		trace.WithSchemaURL(semconv.SchemaURL),
	)
	m.enabled = true
	m.capabilities = map[string]string{
		CapabilityWorkerOpenTelemetryEnabled:       "true",
		CapabilityWorkerOpenTelemetrySchemaVersion: SchemaVersion,
	}
	return m
}

// defaultSpanName is the default WithSpanNameFormatter. It returns
// "function <FunctionName>" -- the OpenTelemetry "verb object"
// convention, and the same format the Java worker emits so cross-runtime
// dashboards searching for function spans find them by the same string.
// Falls back to "azure-functions-invocation" when ic carries no name
// (defensive; should not happen in practice).
func defaultSpanName(ic *sdk.InvocationContext) string {
	if ic == nil || ic.FunctionName == "" {
		return "azure-functions-invocation"
	}
	return "function " + ic.FunctionName
}

// defaultPropagator returns the propagator the middleware uses when the
// caller has not explicitly supplied one via [WithPropagator].
//
// The Functions host always sends W3C-format traceparent/tracestate via
// RpcTraceContext, so we default to a composite propagator that extracts
// W3C TraceContext + Baggage. Importantly, we do NOT delegate to
// otel.GetTextMapPropagator() here: the OTel global default is an empty
// composite that extracts nothing, which would silently break inbound
// trace correlation. Users who want a custom propagator can still pass
// otel.GetTextMapPropagator() explicitly via [WithPropagator].
func defaultPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
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
//   - cloud.provider              = azure
//   - cloud.platform              = azure_functions
//   - cloud.region                = $REGION_NAME (when present)
//   - cloud.resource_id           = full ARM resource ID (when WEBSITE_OWNER_NAME +
//     WEBSITE_RESOURCE_GROUP + WEBSITE_SITE_NAME present)
//   - deployment.environment.name = $WEBSITE_SLOT_NAME (default "production")
//   - service.name                = OTEL_SERVICE_NAME / WEBSITE_SITE_NAME / "azure-functions"
//   - any extras supplied via [WithResource] (highest precedence)
//
// These keys match what the Java worker emits (with one caveat: the
// Java worker still uses the older `cloud.resource.id` form, while
// OTel semconv v1.27 renamed it to `cloud.resource_id` with an
// underscore -- we track the current spec).
//
// resource.Default() already incorporates the standard environment
// detector (OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES) so callers can
// override any of the above with the standard OTel env vars.
func buildDefaultResource(extra ...attribute.KeyValue) *resource.Resource {
	attrs := []attribute.KeyValue{
		semconv.CloudProviderAzure,
		// cloud.platform is hardcoded rather than sourced from
		// semconv.CloudPlatformAzureFunctions because the v1.37.0
		// semconv generator changed the value from "azure_functions"
		// to "azure.functions". The OpenTelemetry spec registry and
		// the dotnet-isolated / Java workers all still emit the
		// underscore form, so we hardcode it here to preserve
		// cross-runtime dashboard filtering. If/when OTel's spec
		// settles on the dotted form across the ecosystem, this can
		// revert to using the semconv constant.
		attribute.String("cloud.platform", "azure_functions"),
	}
	if name := serviceNameFromEnv(); name != "" {
		attrs = append(attrs, semconv.ServiceName(name))
	}
	if region := strings.TrimSpace(os.Getenv(envRegionName)); region != "" {
		attrs = append(attrs, semconv.CloudRegion(region))
	}
	if id := armResourceIDFromEnv(); id != "" {
		attrs = append(attrs, semconv.CloudResourceID(id))
	}
	// deployment.environment.name is always set: WEBSITE_SLOT_NAME when
	// available, "production" as the Azure-canonical default otherwise.
	// Matches the Java worker's behavior.
	slot := strings.TrimSpace(os.Getenv(envWebsiteSlotName))
	if slot == "" {
		slot = "production"
	}
	attrs = append(attrs, semconv.DeploymentEnvironmentName(slot))

	// Caller-supplied attrs come last so they win on duplicate keys
	// (resource.Merge applies right-hand-wins precedence).
	attrs = append(attrs, extra...)

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

// armResourceIDFromEnv builds the full ARM resource ID from the
// Azure Functions environment variables. Returns "" when any required
// input is missing -- callers should skip the attribute in that case.
//
// WEBSITE_OWNER_NAME is shaped "<subscriptionId>+<region-stamp>...",
// so the subscription ID is the prefix up to the first '+'. Matches
// the Java worker's extraction logic exactly so the resulting ID
// string is byte-identical across the two runtimes.
//
// On the Flex Consumption plan, WEBSITE_RESOURCE_GROUP is not populated
// by the platform (it is listed under "Deprecated properties and
// settings" for Flex), so this function returns "" and cloud.resource_id
// is omitted from the Resource. The .NET host's FunctionsResourceDetector
// has the same gap. Customers who need cloud.resource_id on Flex can
// supply it via OTEL_RESOURCE_ATTRIBUTES or [WithResource]; both are
// merged on top of the default Resource by [buildDefaultResource].
func armResourceIDFromEnv() string {
	site := strings.TrimSpace(os.Getenv(envWebsiteSiteName))
	rg := strings.TrimSpace(os.Getenv(envWebsiteResourceGroup))
	owner := strings.TrimSpace(os.Getenv(envWebsiteOwnerName))
	if site == "" || rg == "" || owner == "" {
		return ""
	}
	sub := extractSubscriptionID(owner)
	if sub == "" {
		return ""
	}
	return "/subscriptions/" + sub +
		"/resourceGroups/" + rg +
		"/providers/Microsoft.Web/sites/" + site
}

// extractSubscriptionID parses the subscription GUID out of
// WEBSITE_OWNER_NAME. The format is "<subscriptionId>+<stamp>", so the
// substring before the first '+' is the subscription ID. Returns ""
// when the input is empty or contains no '+'.
func extractSubscriptionID(ownerName string) string {
	if ownerName == "" {
		return ""
	}
	if i := strings.IndexByte(ownerName, '+'); i > 0 {
		return ownerName[:i]
	}
	return ""
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

// Shutdown implements [sdk.ShutdownProvider]. It flushes and releases any
// TracerProvider or LoggerProvider that the middleware constructed itself
// (via WithExporter, WithLogExporter, or env-var auto-config). Providers
// the user supplied via [WithTracerProvider]/[WithLoggerProvider] are NOT
// shut down here — they remain the user's responsibility.
//
// The worker invokes Shutdown after the gRPC stream closes or on
// SIGTERM/SIGINT, so user code does not need a defer cleanup() line in
// main(). Errors from each provider are collected and the first non-nil
// error is returned; the worker logs it and proceeds with process exit.
func (m *otelMiddleware) Shutdown(ctx context.Context) error {
	var firstErr error
	if m.ownedTP != nil {
		if err := m.ownedTP.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if m.ownedLP != nil {
		if err := m.ownedLP.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// exporterInitTimeout bounds the construction of an autoexport OTLP exporter
// so a misbehaving transport (for example, a gRPC dial that blocks) cannot
// stall middleware setup indefinitely. Construction normally returns promptly
// because the OTLP exporters connect lazily on first export.
const exporterInitTimeout = 10 * time.Second

// buildOTLPTracerProvider returns a TracerProvider configured against the
// OTLP HTTP endpoint specified by the standard OpenTelemetry env vars
// (OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_HEADERS,
// OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf). Returns (nil, nil) when no
// endpoint is set so the caller can leave the global TP untouched.
//
// extraResource attributes are merged into the default Resource on top
// of the env-derived defaults so [WithResource] applies to auto-OTLP
// providers exactly as it does to WithExporter-built ones.
func buildOTLPTracerProvider(extraResource ...attribute.KeyValue) (*sdktrace.TracerProvider, error) {
	if !hasOTLPEndpoint() {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), exporterInitTimeout)
	defer cancel()
	exporter, err := autoexport.NewSpanExporter(ctx)
	if err != nil {
		return nil, err
	}
	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(buildDefaultResource(extraResource...)),
	), nil
}

// buildOTLPLoggerProvider returns a LoggerProvider configured against the
// OTLP HTTP endpoint specified by the standard OpenTelemetry env vars.
// Returns (nil, nil) when no endpoint is set so the caller can leave the
// global LP untouched.
func buildOTLPLoggerProvider(extraResource ...attribute.KeyValue) (*sdklog.LoggerProvider, error) {
	if !hasOTLPEndpoint() {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), exporterInitTimeout)
	defer cancel()
	exporter, err := autoexport.NewLogExporter(ctx)
	if err != nil {
		return nil, err
	}
	return sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
		sdklog.WithResource(buildDefaultResource(extraResource...)),
	), nil
}

// hasOTLPEndpoint reports whether the standard OpenTelemetry env vars are
// configured to point at an OTLP collector. We check the generic
// OTEL_EXPORTER_OTLP_ENDPOINT plus the per-signal overrides, matching
// the precedence the official otlphttp/otlpgrpc exporters use internally.
func hasOTLPEndpoint() bool {
	for _, key := range []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
	} {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return true
		}
	}
	return false
}

// isNoopLoggerProvider reports whether the given LoggerProvider is the
// canonical noop returned by go.opentelemetry.io/otel/log/noop. Used to
// decide whether the global is already wired up by the user.
func isNoopLoggerProvider(lp olog.LoggerProvider) bool {
	if lp == nil {
		return true
	}
	_, ok := lp.(lognoop.LoggerProvider)
	return ok
}

// sdkModulePath is the canonical module path of the worker SDK that
// hosts this otelfunc package. resolveSDKVersion walks the user app's
// runtime/debug BuildInfo to find this dep and return its version, used
// as the instrumentation-scope version on emitted spans and log
// records.
const sdkModulePath = "github.com/azure/azure-functions-golang-worker"

// resolveSDKVersion returns the version of the azure-functions-golang-worker
// module the user binary was built against. Cached after first call --
// BuildInfo does not change at runtime.
//
// Return values match the contract documented on worker.WorkerMetadata:
//   - "vX.Y.Z" for tagged releases
//   - "(devel)" for source builds (no tag) and filesystem `replace` directives
//
// Used as the instrumentation-scope version on every Tracer/Logger we
// construct so spans and log records emitted by otelfunc carry
// otel.library.version pointing at the exact SDK build that produced
// them.
var resolveSDKVersion = sync.OnceValue(func() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "(devel)"
	}
	for _, dep := range bi.Deps {
		if dep == nil || dep.Path != sdkModulePath {
			continue
		}
		if dep.Replace != nil && dep.Replace.Version != "" {
			return dep.Replace.Version
		}
		if dep.Version != "" {
			return dep.Version
		}
		break
	}
	return "(devel)"
})

// otelLogObserverOnce ensures the slog->OTel observer is registered at
// most once per process. Multiple registrations would duplicate every
// user log record N times in the configured backend.
var otelLogObserverOnce sync.Once

// registerOTelLogObserverOnce installs an observer on the worker's user
// log handler that emits each record through the otelslog bridge. The
// bridge reads the global OTel LoggerProvider lazily on each Emit call,
// so middleware constructed after another middleware in the same process
// (or before the user calls global.SetLoggerProvider) still routes
// correctly to whichever provider is current.
//
// We register exactly once and rely on the bridge's lazy lookup; there
// is no per-Middleware bookkeeping to do.
func registerOTelLogObserverOnce() {
	otelLogObserverOnce.Do(func() {
		bridge := otelslog.NewHandler(ScopeName,
			otelslog.WithVersion(resolveSDKVersion()),
			otelslog.WithSchemaURL(semconv.SchemaURL),
		)
		log.RegisterObserver(func(ctx context.Context, rec slog.Record) {
			// Bridge errors are swallowed: the RpcLog has already been
			// emitted on the gRPC stream by the time we see the record,
			// so a failed OTel hop loses only the OTel-side copy.
			_ = bridge.Handle(ctx, rec)
		})
	})
}
