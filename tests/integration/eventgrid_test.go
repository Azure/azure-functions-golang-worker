package integration

import (
	"os"
	"strings"
	"testing"
	"time"
)

var eventGridEnv = map[string]string{
	"AzureWebJobsStorage": "UseDevelopmentStorage=true",
}

func TestEventGridTriggerRegisters(t *testing.T) {
	requireAzurite(t)
	// Event Grid has no local emulator. This test only verifies
	// that the function builds, registers, and loads correctly.
	host := startSampleHost(t, "eventGridTrigger", eventGridEnv, 30*time.Second)

	assertHostLogContains(t, host, "eventGridTrigger", 10*time.Second)

	logBytes, err := os.ReadFile(host.LogPath())
	if err != nil {
		t.Fatalf("read host log: %v", err)
	}
	log := string(logBytes)
	lower := strings.ToLower(log)
	// Allow the known benign error from Functions host
	if strings.Contains(lower, "error") && !strings.Contains(log, "Unable to resolve ScriptJobHostOptions") {
		t.Errorf("unexpected error in log:\n%s", log)
	}
}
