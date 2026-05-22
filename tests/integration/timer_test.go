package integration

import (
	"testing"
	"time"
)

var timerEnv = map[string]string{
	"AzureWebJobsStorage":      "UseDevelopmentStorage=true",
	"FUNCTIONS_WORKER_RUNTIME": "golang",
}

func TestTimerTriggerFires(t *testing.T) {
	requireAzurite(t)
	// Timer schedule is */10 * * * * * (every 10 seconds)
	proc := StartFuncHost(t, "timerTrigger", 7203, timerEnv, 30*time.Second)

	proc.AssertLogContains("timer trigger executed", 20*time.Second)
	proc.AssertLogContains("Succeeded", 5*time.Second)
}
