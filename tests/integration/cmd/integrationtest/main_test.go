package main

import (
	"path/filepath"
	"regexp"
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
	if len(runner.Emulators) != 6 {
		t.Fatalf("len(Emulators) = %d, want 6", len(runner.Emulators))
	}
	foundDurableStorage := false
	for _, emulator := range runner.Emulators {
		if emulator.Name == "durable-azurite" {
			foundDurableStorage = emulator.WaitReady != nil && emulator.ArtifactFile == "durable-azurite.log"
		}
	}
	if !foundDurableStorage {
		t.Fatal("runner must own dedicated Durable storage readiness and diagnostics")
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

func TestIntegrationTestPatternSelectsEveryScenario(t *testing.T) {
	pattern := regexp.MustCompile(integrationTestPattern)
	expectedTests := []string{
		"TestHttpTriggerGet",
		"TestTimerTriggerFires",
		"TestBlobTriggerFires",
		"TestQueueStorageTriggerFires",
		"TestQueueStorageTriggerMetadata",
		"TestGenericTriggerQueues",
		"TestGenericTriggerMCP",
		"TestEventGridTriggerRegisters",
		"TestEventHubTriggerFires",
		"TestEventHubTriggerMany",
		"TestServiceBusQueueTriggerFires",
		"TestServiceBusQueueTriggerMany",
		"TestServiceBusTopicTriggerFires",
		"TestServiceBusTopicTriggerMany",
		"TestCosmosDBTriggerFires",
		"TestSQLTriggerFiresOnChanges",
		"TestDurableOrchestrations",
		"TestDurableMiddlewareOrdering",
	}
	for _, testName := range expectedTests {
		if !pattern.MatchString(testName) {
			t.Errorf("integrationTestPattern does not select %s", testName)
		}
		if pattern.MatchString("Prefix"+testName) || pattern.MatchString(testName+"Suffix") {
			t.Errorf("integrationTestPattern is not anchored for %s", testName)
		}
	}
	if pattern.MatchString("TestDefaultRunnerTargetsFullIntegrationSuite") {
		t.Fatal("integrationTestPattern unexpectedly selects runner unit tests")
	}
}
