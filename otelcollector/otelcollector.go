// Package otelcollector embeds an OpenTelemetry Collector inside the Azure
// Functions Go worker process.
//
// The package owns the collector lifecycle so applications don't hand-write
// collector boilerplate: it builds the component factories, resolves a
// configuration, runs the collector, gates on readiness, and shuts it down
// cleanly. A default Azure Monitor configuration is compiled in, so the
// simplest integration is a single option on worker.Start:
//
//	app := sdk.FunctionApp()
//	app.Use(otelfunc.Middleware())
//	app.HTTP("hello", HelloHandler, sdk.WithMethods("GET"))
//	worker.Start(app, otelcollector.WithCollector())
//
// The collector listens for OTLP on localhost:4317 (gRPC) and localhost:4318
// (HTTP) and, with the default config, forwards traces, logs, and metrics to
// an Azure Monitor Data Collection Endpoint using the three OTEL_DCE_*
// environment variables (see default-config.yaml).
//
// # Configuration precedence
//
// When resolving the collector configuration, the following sources are tried
// in order (the env confmap provider is always enabled so ${env:...}
// references expand regardless of source):
//
//  1. Inline YAML supplied via [WithConfigYAML].
//  2. A file path supplied via [WithConfigFile].
//  3. An otel-collector-config.yaml file next to the executable.
//  4. The compiled-in default ([DefaultConfigYAML]).
//
// # Advanced use
//
// Power users who need custom components can build on the bundled factories
// with [DefaultFactories] and pass the augmented set via [WithFactories], or
// drive the collector directly with [Start] and operate on the underlying
// *otelcol.Collector via [Collector.Unwrap].
package otelcollector

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/azureauthextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/confmap/provider/yamlprovider"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
)

//go:embed default-config.yaml
var defaultConfigYAML string

// DefaultConfigFileName is the file name the collector looks for next to the
// executable when no explicit configuration is supplied.
const DefaultConfigFileName = "otel-collector-config.yaml"

// defaultStartTimeout bounds how long [Start] waits for the collector to reach
// the running state before reporting a startup failure.
const defaultStartTimeout = 10 * time.Second

// config holds resolved options for the embedded collector.
type config struct {
	configFile   string
	configYAML   string
	factories    *otelcol.Factories
	failFast     bool
	startTimeout time.Duration
	buildInfo    component.BuildInfo
}

// Option configures the embedded collector. Options are applied in order.
type Option func(*config)

// WithConfigFile resolves the collector configuration from the file at path.
// Takes precedence over the next-to-executable file and the embedded default,
// but not over [WithConfigYAML].
func WithConfigFile(path string) Option {
	return func(c *config) { c.configFile = path }
}

// WithConfigYAML resolves the collector configuration from inline YAML. This
// has the highest precedence among configuration sources. ${env:...}
// references are still expanded.
func WithConfigYAML(yaml string) Option {
	return func(c *config) { c.configYAML = yaml }
}

// WithFactories replaces the component factory set used to build the
// collector. Use [DefaultFactories] as a base to extend the bundled
// components, for example:
//
//	f, _ := otelcollector.DefaultFactories()
//	f.Receivers[myrecv.NewFactory().Type()] = myrecv.NewFactory()
//	worker.Start(app, otelcollector.WithCollector(otelcollector.WithFactories(f)))
func WithFactories(f otelcol.Factories) Option {
	return func(c *config) { c.factories = &f }
}

// FailFast makes a collector startup failure fatal when used via
// [WithCollector]: the worker logs the error and terminates. By default the
// hook degrades gracefully, logging a warning and continuing without the
// collector. FailFast has no effect on the direct [Start] API, which always
// returns its error.
func FailFast() Option {
	return func(c *config) { c.failFast = true }
}

// StartTimeout overrides how long [Start] waits for the collector to reach the
// running state. A non-positive duration restores the default.
func StartTimeout(d time.Duration) Option {
	return func(c *config) { c.startTimeout = d }
}

// WithBuildInfo overrides the BuildInfo reported by the embedded collector.
func WithBuildInfo(bi component.BuildInfo) Option {
	return func(c *config) { c.buildInfo = bi }
}

func newConfig(opts []Option) config {
	c := config{
		startTimeout: defaultStartTimeout,
		buildInfo: component.BuildInfo{
			Command:     "embedded-otelcol",
			Description: "Embedded OTel Collector for the Azure Functions Go worker",
			Version:     "0.1.0",
		},
	}
	for _, o := range opts {
		o(&c)
	}
	if c.startTimeout <= 0 {
		c.startTimeout = defaultStartTimeout
	}
	return c
}

// DefaultFactories returns the component factories bundled with the worker:
//
//   - receivers:   otlp
//   - processors:  batch, cumulativetodelta
//   - exporters:   otlphttp
//   - extensions:  azureauth
//
// The returned set is a fresh copy and safe to mutate, making it a convenient
// base for [WithFactories].
func DefaultFactories() (otelcol.Factories, error) {
	otlpRecv := otlpreceiver.NewFactory()
	batchProc := batchprocessor.NewFactory()
	cumulativeToDelta := cumulativetodeltaprocessor.NewFactory()
	otlpHTTPExp := otlphttpexporter.NewFactory()
	azureAuthExt := azureauthextension.NewFactory()

	return otelcol.Factories{
		Receivers: map[component.Type]receiver.Factory{
			otlpRecv.Type(): otlpRecv,
		},
		Processors: map[component.Type]processor.Factory{
			batchProc.Type():         batchProc,
			cumulativeToDelta.Type(): cumulativeToDelta,
		},
		Exporters: map[component.Type]exporter.Factory{
			otlpHTTPExp.Type(): otlpHTTPExp,
		},
		Extensions: map[component.Type]extension.Factory{
			azureAuthExt.Type(): azureAuthExt,
		},
		Connectors: map[component.Type]connector.Factory{},
	}, nil
}

// DefaultConfigYAML returns the compiled-in default collector configuration.
func DefaultConfigYAML() string {
	return defaultConfigYAML
}

// Collector is a running embedded OpenTelemetry Collector. Its lifecycle is
// managed by [Start]/[Collector.Shutdown] (or, transparently, by
// [WithCollector]).
type Collector struct {
	col    *otelcol.Collector
	cancel context.CancelFunc
	done   chan error
}

// Start builds and runs an embedded collector, blocking until it reaches the
// running state or the start timeout elapses. The returned [Collector] must be
// shut down via [Collector.Shutdown] to flush and release resources.
//
// The supplied ctx bounds the readiness wait only; the collector itself runs
// until [Collector.Shutdown] is called.
func Start(ctx context.Context, opts ...Option) (*Collector, error) {
	return start(ctx, newConfig(opts))
}

func start(ctx context.Context, cfg config) (*Collector, error) {
	factories := cfg.factories
	if factories == nil {
		f, err := DefaultFactories()
		if err != nil {
			return nil, fmt.Errorf("build collector factories: %w", err)
		}
		factories = &f
	}

	uris, providers, source := resolveConfig(cfg)
	slog.LogAttrs(ctx, slog.LevelInfo, "embedded collector configuration resolved",
		slog.String("source", source),
	)

	settings := otelcol.CollectorSettings{
		BuildInfo: cfg.buildInfo,
		Factories: func() (otelcol.Factories, error) { return *factories, nil },
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs:              uris,
				ProviderFactories: providers,
			},
		},
	}

	col, err := otelcol.NewCollector(settings)
	if err != nil {
		return nil, fmt.Errorf("create collector: %w", err)
	}

	// Run the collector on its own background context so it outlives the
	// readiness-gating ctx and stops only on Shutdown (or explicit cancel).
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- col.Run(runCtx) }()

	c := &Collector{col: col, cancel: cancel, done: done}

	alive, err := c.waitReady(ctx, cfg.startTimeout)
	if err != nil {
		cancel()
		// When readiness fails on a timeout or context cancellation the run
		// goroutine is still executing col.Run; cancel() above signals it to
		// stop. Drain done (bounded) so Start does not return while a
		// partially-started collector goroutine is still running. When the
		// collector already exited on its own, alive is false and done has
		// been consumed by waitReady, so there is nothing to drain.
		if alive {
			select {
			case <-done:
			case <-time.After(cfg.startTimeout):
			}
		}
		return nil, err
	}
	return c, nil
}

// waitReady blocks until the collector reports the running state, the timeout
// elapses, the collector exits, or ctx is cancelled. The returned alive flag
// reports whether the collector run goroutine is still executing when waitReady
// returns: true on success and on timeout/cancellation (the goroutine keeps
// running col.Run), false when the collector has already exited (done has been
// consumed). Callers use alive to decide whether the goroutine still needs to
// be drained after cancelling it.
func (c *Collector) waitReady(ctx context.Context, timeout time.Duration) (alive bool, err error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case err := <-c.done:
			if err != nil {
				return false, fmt.Errorf("collector exited during startup: %w", err)
			}
			return false, fmt.Errorf("collector stopped during startup")
		case <-deadline.C:
			return true, fmt.Errorf("collector did not reach running state within %s", timeout)
		case <-ctx.Done():
			return true, ctx.Err()
		case <-tick.C:
			if c.col.GetState() == otelcol.StateRunning {
				return true, nil
			}
		}
	}
}

// Shutdown gracefully stops the collector, flushing buffered telemetry, and
// waits for it to exit. It is bounded by ctx. Shutdown is safe to call on a
// nil *Collector (a no-op), which lets graceful-degrade callers avoid a guard.
func (c *Collector) Shutdown(ctx context.Context) error {
	if c == nil || c.col == nil {
		return nil
	}
	c.col.Shutdown()
	select {
	case err := <-c.done:
		c.cancel()
		if err != nil {
			return fmt.Errorf("collector shutdown: %w", err)
		}
		return nil
	case <-ctx.Done():
		c.cancel()
		return ctx.Err()
	}
}

// Unwrap exposes the underlying *otelcol.Collector for advanced inspection.
// Callers should not Run or Shutdown it directly; use [Collector.Shutdown].
func (c *Collector) Unwrap() *otelcol.Collector {
	if c == nil {
		return nil
	}
	return c.col
}

// resolveConfig determines the collector configuration URIs and providers
// based on the configured precedence. The env provider is always included so
// ${env:...} references expand regardless of the base config source.
func resolveConfig(cfg config) (uris []string, providers []confmap.ProviderFactory, source string) {
	providers = []confmap.ProviderFactory{
		fileprovider.NewFactory(),
		envprovider.NewFactory(),
		yamlprovider.NewFactory(),
	}

	switch {
	case cfg.configYAML != "":
		return []string{"yaml:" + cfg.configYAML}, providers, "inline yaml (WithConfigYAML)"
	case cfg.configFile != "":
		return []string{"file:" + cfg.configFile}, providers, "file (WithConfigFile): " + cfg.configFile
	}

	if execPath, err := os.Executable(); err == nil {
		candidate := filepath.Join(filepath.Dir(execPath), DefaultConfigFileName)
		if _, statErr := os.Stat(candidate); statErr == nil {
			return []string{"file:" + candidate}, providers, "file next to executable: " + candidate
		}
	}

	return []string{"yaml:" + defaultConfigYAML}, providers, "embedded default"
}

// WithCollector returns an [sdk.StartOption] that runs an embedded OTel
// Collector for the lifetime of the worker: it starts (and readiness-gates)
// before the worker serves and shuts down (flushing) during worker teardown.
//
// By default a startup failure degrades gracefully — the worker logs a warning
// and continues without the collector. Pass [FailFast] to make startup failure
// terminate the worker instead.
func WithCollector(opts ...Option) sdk.StartOption {
	return sdk.WithLifecycleHook(&hook{opts: opts})
}

// hook adapts the embedded collector to the worker's lifecycle.
type hook struct {
	opts []Option
	col  *Collector
}

func (h *hook) Start(ctx context.Context) error {
	cfg := newConfig(h.opts)
	col, err := start(ctx, cfg)
	if err != nil {
		if cfg.failFast {
			return fmt.Errorf("embedded collector: %w", err)
		}
		slog.LogAttrs(ctx, slog.LevelWarn, "embedded collector failed to start; continuing without it",
			slog.Any("err", err),
		)
		return nil
	}
	h.col = col
	return nil
}

func (h *hook) Shutdown(ctx context.Context) error {
	return h.col.Shutdown(ctx)
}
