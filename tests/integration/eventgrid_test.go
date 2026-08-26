package integration

import (
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

	assertHostLogNotContainsError(t, host,
		"Unable to resolve ScriptJobHostOptions",
		"ConsecutiveErrors=",
		`"UseStdErrorStreamForErrorsOnly"`,
	)
}
