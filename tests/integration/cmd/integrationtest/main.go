package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/azure/azure-functions-golang-worker/tests/integration/internal/testrunner"
)

// suiteTimeout bounds the complete integration suite, including prerequisite
// checks, emulator startup, test execution, diagnostics, and cleanup.
const suiteTimeout = 20 * time.Minute

const integrationTestPattern = "^(TestHttpTriggerGet|" +
	"TestTimerTriggerFires|" +
	"TestBlobTriggerFires|" +
	"TestQueueStorageTriggerFires|" +
	"TestQueueStorageTriggerMetadata|" +
	"TestEventGridTriggerRegisters|" +
	"TestEventHubTriggerFires|" +
	"TestEventHubTriggerMany|" +
	"TestServiceBusQueueTriggerFires|" +
	"TestServiceBusQueueTriggerMany|" +
	"TestServiceBusTopicTriggerFires|" +
	"TestServiceBusTopicTriggerMany|" +
	"TestCosmosDBTriggerFires|" +
	"TestSQLTriggerFiresOnChanges|" +
	"TestDurableOrchestrations)$"

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), suiteTimeout)
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
		ComposeFile:             filepath.Join("..", "emulators", "docker-compose.yml"),
		ArtifactDir:             "artifacts",
		TestPattern:             integrationTestPattern,
		FuncExe:                 funcExe,
		MinimumCoreToolsVersion: "4.12.0",
		Emulators:               testrunner.DefaultEmulators(),
	}
}
