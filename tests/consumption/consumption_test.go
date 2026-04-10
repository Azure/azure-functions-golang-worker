package consumption

import (
	"testing"
	"time"
)

func TestFlexConsumptionPlaceholderPing(t *testing.T) {
	requireDocker(t)

	fc := startFlexContainer(t)

	// In placeholder mode, /admin/host/ping should return 200
	fc.waitForPing(60 * time.Second)
}

func TestFlexConsumptionHttpTrigger(t *testing.T) {
	requireDocker(t)

	zipPath := buildSampleZip(t, "httpTrigger")

	fc := startFlexContainer(t)
	fc.waitForPing(60 * time.Second)

	// Deploy app + worker.config.json into the container
	fc.deployApp(zipPath)

	// Specialize — host restarts, discovers worker.config.json and app binary
	fc.specialize(map[string]string{
		"AzureWebJobsStorage": "UseDevelopmentStorage=true",
	})

	status, body := fc.sendRequest("GET", "/api/hello")
	if status != 200 {
		t.Fatalf("expected 200 from /api/hello, got %d", status)
	}
	if string(body) != "Hello from Go Worker!" {
		t.Fatalf("expected 'Hello from Go Worker!', got %q", string(body))
	}
}

// TestFlexConsumptionHttpTriggerIdealImage demonstrates the host limitation:
// when worker.config.json is baked into the image with FUNCTIONS_WORKER_RUNTIME=native,
// the host tries to start the worker during placeholder mode and fatally fails
// because the binary doesn't exist yet. This test is expected to FAIL.
// Once the host gracefully handles missing worker binaries in placeholder mode,
// this test should pass and the workaround test below becomes unnecessary.
func TestFlexConsumptionHttpTriggerIdealImage(t *testing.T) {
	requireDocker(t)

	zipPath := buildSampleZipMinimal(t, "httpTrigger")

	fc := startFlexContainerIdeal(t)
	fc.waitForPing(60 * time.Second)

	fc.deployApp(zipPath)

	fc.specialize(map[string]string{
		"AzureWebJobsStorage": "UseDevelopmentStorage=true",
	})

	status, body := fc.sendRequest("GET", "/api/hello")
	if status != 200 {
		t.Fatalf("expected 200 from /api/hello, got %d", status)
	}
	if string(body) != "Hello from Go Worker!" {
		t.Fatalf("expected 'Hello from Go Worker!', got %q", string(body))
	}
}

// TestFlexConsumptionHttpTriggerIdealImageWorkaround works around the host
// limitation above by not setting FUNCTIONS_WORKER_RUNTIME in the image, so the
// host skips worker init during placeholder. The runtime is sent during
// specialization instead. This is the current viable path for the ideal image
// layout until the host is fixed.
func TestFlexConsumptionHttpTriggerIdealImageWorkaround(t *testing.T) {
	requireDocker(t)

	zipPath := buildSampleZipMinimal(t, "httpTrigger")

	fc := startFlexContainerIdealWorkaround(t)
	fc.waitForPing(60 * time.Second)

	fc.deployApp(zipPath)

	// Must send FUNCTIONS_WORKER_RUNTIME during specialization since the
	// image intentionally doesn't set it.
	fc.specialize(map[string]string{
		"AzureWebJobsStorage":              "UseDevelopmentStorage=true",
		"FUNCTIONS_WORKER_RUNTIME":         "native",
		"FUNCTIONS_WORKER_RUNTIME_VERSION": "1.0",
	})

	status, body := fc.sendRequest("GET", "/api/hello")
	if status != 200 {
		t.Fatalf("expected 200 from /api/hello, got %d", status)
	}
	if string(body) != "Hello from Go Worker!" {
		t.Fatalf("expected 'Hello from Go Worker!', got %q", string(body))
	}
}
