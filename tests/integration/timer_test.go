package integration

import (
	"testing"
	"time"
)

var timerEnv = map[string]string{
	"AzureWebJobsStorage": "UseDevelopmentStorage=true",
}

func TestTimerTriggerFires(t *testing.T) {
	requireAzurite(t)
	// Timer schedule is */10 * * * * * (every 10 seconds)
	host := startSampleHost(t, "timerTrigger", timerEnv, 30*time.Second)

	assertHostLogContains(t, host, "timer trigger executed", 20*time.Second)
	assertHostLogContains(t, host, "Succeeded", 5*time.Second)
}
