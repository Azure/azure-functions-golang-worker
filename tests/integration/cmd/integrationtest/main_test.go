package main

import (
	"path/filepath"
	"testing"
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
	if runner.CoreToolsVersion != "4.12.0" {
		t.Fatalf("CoreToolsVersion = %q", runner.CoreToolsVersion)
	}
}
