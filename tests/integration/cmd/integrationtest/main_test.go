package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultRunnerTargetsHTTPAzuriteMilestone(t *testing.T) {
	t.Setenv("FUNC_EXE", "")
	runner := defaultRunner()

	if runner.ComposeFile != filepath.Join("..", "emulators", "docker-compose.yml") {
		t.Fatalf("ComposeFile = %q", runner.ComposeFile)
	}
	if runner.ArtifactDir != "artifacts" {
		t.Fatalf("ArtifactDir = %q", runner.ArtifactDir)
	}
	if runner.TestPattern != "^TestHttpTriggerGet$" {
		t.Fatalf("TestPattern = %q", runner.TestPattern)
	}
	if runner.FuncExe != "func" {
		t.Fatalf("FuncExe = %q", runner.FuncExe)
	}
	if runner.MinimumCoreToolsVersion != "4.12.0" {
		t.Fatalf("MinimumCoreToolsVersion = %q", runner.MinimumCoreToolsVersion)
	}
	if suiteTimeout != 3*time.Minute {
		t.Fatalf("suiteTimeout = %s", suiteTimeout)
	}
}
