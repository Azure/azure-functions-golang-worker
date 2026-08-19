package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultRunnerTargetsFullIntegrationSuite(t *testing.T) {
	t.Setenv("FUNC_EXE", "")
	runner := defaultRunner()

	if runner.ComposeFile != filepath.Join("..", "emulators", "docker-compose.yml") {
		t.Fatalf("ComposeFile = %q", runner.ComposeFile)
	}
	if runner.ArtifactDir != "artifacts" {
		t.Fatalf("ArtifactDir = %q", runner.ArtifactDir)
	}
	if runner.TestPattern != integrationTestPattern {
		t.Fatalf("TestPattern = %q", runner.TestPattern)
	}
	if len(runner.Emulators) != 5 {
		t.Fatalf("len(Emulators) = %d, want 5", len(runner.Emulators))
	}
	if runner.FuncExe != "func" {
		t.Fatalf("FuncExe = %q", runner.FuncExe)
	}
	if runner.MinimumCoreToolsVersion != "4.12.0" {
		t.Fatalf("MinimumCoreToolsVersion = %q", runner.MinimumCoreToolsVersion)
	}
	if suiteTimeout != 20*time.Minute {
		t.Fatalf("suiteTimeout = %s", suiteTimeout)
	}
}
