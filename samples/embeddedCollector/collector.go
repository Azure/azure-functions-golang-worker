package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/azureauthextension"
)

// startCollector builds and runs an embedded OTel Collector in the background.
// The collector reads its config from otel-collector-config.yaml and exposes
// an OTLP receiver on localhost:4318. Both the Functions host and the Go worker
// export telemetry to this endpoint. The collector handles Azure AD
// authentication and forwards data to Azure Monitor DCE endpoints.
func startCollector(ctx context.Context) (*otelcol.Collector, error) {
	factories, err := buildFactories()
	if err != nil {
		return nil, fmt.Errorf("build collector factories: %w", err)
	}

	// Resolve config path relative to the executable so it works regardless of CWD.
	execPath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve executable path: %w", err)
	}
	configPath := filepath.Join(filepath.Dir(execPath), "otel-collector-config.yaml")

	settings := otelcol.CollectorSettings{
		BuildInfo: component.BuildInfo{
			Command:     "embedded-otelcol",
			Description: "Embedded OTel Collector for Azure Functions Go Worker",
			Version:     "0.1.0",
		},
		Factories: func() (otelcol.Factories, error) { return factories, nil },
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{"file:" + configPath},
				ProviderFactories: []confmap.ProviderFactory{
					fileprovider.NewFactory(),
					envprovider.NewFactory(),
				},
			},
		},
	}

	col, err := otelcol.NewCollector(settings)
	if err != nil {
		return nil, fmt.Errorf("create collector: %w", err)
	}

	go func() {
		if err := col.Run(ctx); err != nil {
			fmt.Printf("collector exited: %v\n", err)
		}
	}()

	return col, nil
}

func buildFactories() (otelcol.Factories, error) {
	otlpRecvFactory := otlpreceiver.NewFactory()
	batchProcFactory := batchprocessor.NewFactory()
	otlpHTTPExpFactory := otlphttpexporter.NewFactory()
	azureAuthExtFactory := azureauthextension.NewFactory()

	return otelcol.Factories{
		Receivers: map[component.Type]receiver.Factory{
			otlpRecvFactory.Type(): otlpRecvFactory,
		},
		Processors: map[component.Type]processor.Factory{
			batchProcFactory.Type(): batchProcFactory,
		},
		Exporters: map[component.Type]exporter.Factory{
			otlpHTTPExpFactory.Type(): otlpHTTPExpFactory,
		},
		Extensions: map[component.Type]extension.Factory{
			azureAuthExtFactory.Type(): azureAuthExtFactory,
		},
		Connectors: map[component.Type]connector.Factory{},
	}, nil
}
