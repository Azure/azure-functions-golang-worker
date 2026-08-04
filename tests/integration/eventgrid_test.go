package integration

import (
	"strings"
	"testing"
	"time"
)

var eventGridEnv = map[string]string{
	"AzureWebJobsStorage":      "UseDevelopmentStorage=true",
}

func TestEventGridTriggerRegisters(t *testing.T) {
	requireAzurite(t)
	// Event Grid has no local emulator. This test only verifies
	// that the function builds, registers, and loads correctly.
	proc := StartFuncHost(t, "eventGridTrigger", 7209, eventGridEnv, 30*time.Second)

	proc.AssertLogContains("eventGridTrigger", 10*time.Second)

	log := proc.ReadLog()
	lower := strings.ToLower(log)
	// Allow the known benign error from Functions host
	if strings.Contains(lower, "error") && !strings.Contains(log, "Unable to resolve ScriptJobHostOptions") {
		t.Errorf("unexpected error in log:\n%s", log)
	}
}
