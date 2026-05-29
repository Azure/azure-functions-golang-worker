package otelcollector

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
)

func TestDefaultConfigYAML_NotEmpty(t *testing.T) {
	got := DefaultConfigYAML()
	if strings.TrimSpace(got) == "" {
		t.Fatal("embedded default config is empty")
	}
	for _, want := range []string{"otlp", "azuremonitor", "cumulativetodelta", "OTEL_DCE_TRACES_ENDPOINT"} {
		if !strings.Contains(got, want) {
			t.Errorf("default config missing %q", want)
		}
	}
}

func TestDefaultFactories(t *testing.T) {
	f, err := DefaultFactories()
	if err != nil {
		t.Fatalf("DefaultFactories: %v", err)
	}
	if len(f.Receivers) != 1 {
		t.Errorf("expected 1 receiver, got %d", len(f.Receivers))
	}
	if len(f.Processors) != 2 {
		t.Errorf("expected 2 processors (batch, cumulativetodelta), got %d", len(f.Processors))
	}
	if len(f.Exporters) != 1 {
		t.Errorf("expected 1 exporter, got %d", len(f.Exporters))
	}
	if len(f.Extensions) != 1 {
		t.Errorf("expected 1 extension (azureauth), got %d", len(f.Extensions))
	}
}

func TestNewConfig_Defaults(t *testing.T) {
	c := newConfig(nil)
	if c.startTimeout != defaultStartTimeout {
		t.Errorf("startTimeout = %v, want %v", c.startTimeout, defaultStartTimeout)
	}
	if c.failFast {
		t.Errorf("failFast should default to false")
	}
	if c.buildInfo.Command != "embedded-otelcol" {
		t.Errorf("unexpected build command %q", c.buildInfo.Command)
	}
}

func TestNewConfig_Options(t *testing.T) {
	c := newConfig([]Option{
		WithConfigYAML("yaml-content"),
		FailFast(),
		StartTimeout(-1), // non-positive restores default
	})
	if c.configYAML != "yaml-content" {
		t.Errorf("configYAML = %q", c.configYAML)
	}
	if !c.failFast {
		t.Errorf("FailFast not applied")
	}
	if c.startTimeout != defaultStartTimeout {
		t.Errorf("non-positive StartTimeout should restore default, got %v", c.startTimeout)
	}
}

func TestResolveConfig_Precedence(t *testing.T) {
	t.Run("inline yaml wins", func(t *testing.T) {
		uris, providers, source := resolveConfig(newConfig([]Option{
			WithConfigYAML("foo: bar"),
			WithConfigFile("/some/file.yaml"),
		}))
		if len(uris) != 1 || uris[0] != "yaml:foo: bar" {
			t.Errorf("unexpected uris %v", uris)
		}
		if !strings.Contains(source, "inline yaml") {
			t.Errorf("source = %q", source)
		}
		if len(providers) != 3 {
			t.Errorf("expected file+env+yaml providers, got %d", len(providers))
		}
	})

	t.Run("file when no inline", func(t *testing.T) {
		uris, _, source := resolveConfig(newConfig([]Option{WithConfigFile("/some/file.yaml")}))
		if len(uris) != 1 || uris[0] != "file:/some/file.yaml" {
			t.Errorf("unexpected uris %v", uris)
		}
		if !strings.Contains(source, "WithConfigFile") {
			t.Errorf("source = %q", source)
		}
	})

	t.Run("embedded default fallback", func(t *testing.T) {
		// With no options and (presumably) no config file next to the test
		// binary, resolution falls back to the embedded default.
		uris, _, source := resolveConfig(newConfig(nil))
		if len(uris) != 1 || !strings.HasPrefix(uris[0], "yaml:") {
			t.Errorf("unexpected uris %v", uris)
		}
		if !strings.Contains(source, "embedded default") && !strings.Contains(source, "next to executable") {
			t.Errorf("source = %q", source)
		}
	})
}

// minimalConfig is a self-contained collector config that needs no network or
// credentials at startup: an OTLP receiver on ephemeral ports feeding an
// otlphttp exporter (which connects lazily, only when data flows).
const minimalConfig = `
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: localhost:0
      http:
        endpoint: localhost:0
exporters:
  otlphttp:
    endpoint: http://localhost:4318
service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlphttp]
`

func TestStart_ReadyAndShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	col, err := Start(ctx, WithConfigYAML(minimalConfig), StartTimeout(15*time.Second))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if col.Unwrap() == nil {
		t.Errorf("Unwrap returned nil for a running collector")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := col.Shutdown(shutdownCtx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

func TestStart_InvalidConfigErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := Start(ctx, WithConfigYAML("this: is: not: valid: collector: config"))
	if err == nil {
		t.Fatal("expected error for invalid config, got nil")
	}
}

func TestNilCollectorShutdown(t *testing.T) {
	var c *Collector
	if err := c.Shutdown(context.Background()); err != nil {
		t.Errorf("nil Collector Shutdown should be a no-op, got %v", err)
	}
	if c.Unwrap() != nil {
		t.Errorf("nil Collector Unwrap should be nil")
	}
}

func TestWithCollector_DegradesByDefault(t *testing.T) {
	// A bad config should make the hook degrade (return nil) by default, and
	// fail (return error) when FailFast is set.
	bad := "not: [valid"

	degrade := hookFromOption(t, WithCollector(WithConfigYAML(bad)))
	if err := degrade.Start(context.Background()); err != nil {
		t.Errorf("default hook should degrade on bad config, got error: %v", err)
	}
	// Shutting down a hook that never started is a no-op.
	if err := degrade.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown after degrade should be nil, got %v", err)
	}

	failFast := hookFromOption(t, WithCollector(WithConfigYAML(bad), FailFast()))
	if err := failFast.Start(context.Background()); err == nil {
		t.Error("FailFast hook should return an error on bad config")
	}
}

// hookFromOption applies a StartOption and returns the single registered hook.
func hookFromOption(t *testing.T, opt sdk.StartOption) sdk.LifecycleHook {
	t.Helper()
	var cfg sdk.StartConfig
	opt(&cfg)
	if len(cfg.Hooks) != 1 {
		t.Fatalf("expected exactly 1 hook, got %d", len(cfg.Hooks))
	}
	return cfg.Hooks[0]
}

func TestDefaultConfigFileName(t *testing.T) {
	// Guard against accidental rename: the convention is documented in the
	// package doc and relied on by resolveConfig.
	if filepath.Base(DefaultConfigFileName) != "otel-collector-config.yaml" {
		t.Errorf("DefaultConfigFileName = %q", DefaultConfigFileName)
	}
}
