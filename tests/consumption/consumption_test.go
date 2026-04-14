package consumption

import (
	"testing"
	"time"
)

func TestFlexConsumptionPlaceholderPing(t *testing.T) {
	requireDocker(t)

	fc := buildAndStartFlex(t, "Dockerfile.flex-test", "goworker-flex-test:latest")

	// In placeholder mode, /admin/host/ping should return 200
	fc.waitForPing(60 * time.Second)
}

func TestFlexConsumptionHttpTriggerProxy(t *testing.T) {
	requireDocker(t)

	zipPath := buildSampleZipMinimal(t, "httpTrigger")

	fc := buildAndStartFlex(t, "Dockerfile.flex-test", "goworker-flex-test:latest")
	fc.waitForPing(60 * time.Second)

	fc.deployApp(zipPath)

	fc.specialize(map[string]string{
		"AzureWebJobsStorage": "UseDevelopmentStorage=true",
	})

	status, body := fc.sendRequest("GET", "/api/hello")
	if status != 200 {
		t.Fatalf("expected 200 from /api/hello, got %d\nlogs:\n%s", status, fc.logs())
	}
	if string(body) != "Hello from Go Worker!" {
		t.Fatalf("expected 'Hello from Go Worker!', got %q", string(body))
	}
}
