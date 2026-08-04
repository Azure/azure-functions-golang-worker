package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/azure/azure-functions-golang-worker/tests/integration/internal/testrunner"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	if err := defaultRunner().Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func defaultRunner() testrunner.Runner {
	funcExe := os.Getenv("FUNC_EXE")
	if funcExe == "" {
		funcExe = "func"
	}
	return testrunner.Runner{
		ComposeFile:      filepath.Join("..", "emulators", "docker-compose.yml"),
		ArtifactDir:      "artifacts",
		TestPattern:      "^TestHttpTriggerGet$",
		FuncExe:          funcExe,
		CoreToolsVersion: "4.12.0",
	}
}
