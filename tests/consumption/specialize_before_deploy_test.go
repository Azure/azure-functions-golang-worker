package consumption

import (
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// azuriteConnString returns the Azurite connection string using the container's
// Docker bridge IP, so the test container can reach it directly.
func azuriteConnString(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("docker", "inspect", "azurite",
		"--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}").CombinedOutput()
	if err != nil {
		t.Skipf("Azurite container not found: %v\n%s", err, out)
	}
	ip := strings.TrimSpace(string(out))
	return fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"+
			"AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"+
			"BlobEndpoint=http://%s:10000/devstoreaccount1;"+
			"QueueEndpoint=http://%s:10001/devstoreaccount1;"+
			"TableEndpoint=http://%s:10002/devstoreaccount1;",
		ip, ip, ip)
}

// TestSpecializeBeforeDeploy verifies that a container started post-specialization
// without an app binary remains healthy instead of crash-looping.
//
// Production scenario: A function app is created on flex consumption. The
// container is assigned and specialized, but the user hasn't deployed code yet.
// The host starts with WEBSITE_PLACEHOLDER_MODE=0, launches the proxy, and the
// proxy should stay alive as a no-op worker (zero functions).
//
// Before the fix, the proxy would fatal at startup with "Proxy requires
// WEBSITE_PLACEHOLDER_MODE=1", causing a tight crash loop where the host
// repeatedly restarted the script host (ConsecutiveErrors=0,1,2,...).
//
// Uses the MCR native1.0 image with the proxy replaced by a local build.
func TestSpecializeBeforeDeploy(t *testing.T) {
	requireDocker(t)

	connStr := azuriteConnString(t)

	// Start the container already specialized (WEBSITE_PLACEHOLDER_MODE=0),
	// with a real storage connection, and no app binary.
	fc := buildAndStartFlexSpecialized(t,
		"Dockerfile.flex-test-specialize-before-deploy",
		"goworker-specialize-before-deploy:latest",
		map[string]string{
			"AzureWebJobsStorage":        connStr,
			"FUNCTIONS_EXTENSION_VERSION": "~4",
			"FUNCTIONS_WORKER_RUNTIME":    "native",
		},
	)

	// Wait for the host to start and the proxy to initialize.
	t.Log("Waiting for host to start with proxy as no-op worker...")
	time.Sleep(30 * time.Second)

	// Verify the container is still running.
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", fc.id).CombinedOutput()
	if err != nil || strings.TrimSpace(string(out)) != "true" {
		t.Fatalf("container is not running\nrunning=%s\nlogs:\n%s",
			strings.TrimSpace(string(out)), fc.logs())
	}

	logs := fc.logs()

	// Verify the proxy logged the no-op message instead of crashing.
	if strings.Contains(logs, "Running as no-op worker") {
		t.Log("CONFIRMED: Proxy is running as no-op worker (no crash)")
	} else {
		t.Errorf("Expected 'Running as no-op worker' in logs but not found")
	}

	// Verify no crash loop: there should be NO ConsecutiveErrors from the
	// host restarting due to worker failures.
	if strings.Contains(logs, "ConsecutiveErrors=1") ||
		strings.Contains(logs, "ConsecutiveErrors=2") {
		t.Errorf("Unexpected ConsecutiveErrors in logs — proxy should not be crash-looping")
	} else {
		t.Log("CONFIRMED: No crash loop (no ConsecutiveErrors)")
	}

	// Verify the old fatal message does NOT appear.
	if strings.Contains(logs, "Proxy requires WEBSITE_PLACEHOLDER_MODE=1") {
		t.Errorf("Old fatal message found — proxy should not crash without placeholder mode")
	} else {
		t.Log("CONFIRMED: No fatal placeholder mode message")
	}

	t.Logf("Full container logs:\n%s", logs)
}

// TestSpecializeBeforeDeployFullFlow tests the complete production lifecycle
// for a newly created function app where code is deployed after the container
// is already specialized.
//
// Production flow (from Kudu Legion source):
//
//  1. Function app is created → container assigned → specialized (no app binary)
//  2. Proxy handles FERR, responds with success and empty functions metadata.
//     Host is specialized, healthy, zero functions. Container stays alive.
//  3. User deploys code via `func azure functionapp publish` or ARM
//  4. Kudu uploads package → platform kills all worker pods (removeworker/allStandard)
//  5. New container starts fresh → specialization → binary exists from FUSE mount
//  6. /api/hello returns 200
//
// This test covers steps 1-2, then verifies the container is healthy.
// Steps 3-6 are effectively covered by existing tests (e.g. consumption_test.go)
// that test normal specialization with a pre-baked app.
//
// Before the fix, step 2 would crash the proxy (log.Fatalf in specialize()),
// making the container unhealthy. The platform would find an unhealthy pod
// and the user would see errors.
func TestSpecializeBeforeDeployFullFlow(t *testing.T) {
	requireDocker(t)

	connStr := azuriteConnString(t)

	// Start in placeholder mode with no app pre-baked.
	fc := buildAndStartFlex(t,
		"Dockerfile.flex-test-specialize-before-deploy",
		"goworker-specialize-before-deploy-full:latest")
	fc.waitForPing(90 * time.Second)

	// Specialize with a real Azurite connection but no app deployed yet.
	// With the fix, the proxy responds to FERR with success and returns
	// empty functions metadata — the host proceeds normally.
	t.Log("Specializing without app deployed...")
	fc.specialize(map[string]string{
		"AzureWebJobsStorage": connStr,
	})

	// Give the host time to complete specialization.
	time.Sleep(30 * time.Second)

	// Verify container is still running and healthy (proxy didn't crash-loop).
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", fc.id).CombinedOutput()
	if err != nil || strings.TrimSpace(string(out)) != "true" {
		t.Fatalf("container is not running after specialize without app\nrunning=%s\nlogs:\n%s",
			strings.TrimSpace(string(out)), fc.logs())
	}
	t.Log("Container still running after specialize without app")

	logs := fc.logs()

	// Verify the proxy handled the FERR gracefully.
	if strings.Contains(logs, "App binary not found") &&
		strings.Contains(logs, "running as no-op worker") {
		t.Log("CONFIRMED: Proxy handled FERR without binary gracefully")
	} else {
		t.Errorf("Expected no-op worker message during specialization")
	}

	// Verify no crash: the old fatal should not appear.
	if strings.Contains(logs, "Failed to start child worker") {
		t.Errorf("Proxy crashed in specialize() — fix did not work")
	} else {
		t.Log("CONFIRMED: No crash in specialize()")
	}

	// Verify host completed specialization without errors.
	if strings.Contains(logs, "ConsecutiveErrors=1") {
		t.Errorf("Host has ConsecutiveErrors — worker is crash-looping")
	} else {
		t.Log("CONFIRMED: Host has no ConsecutiveErrors")
	}

	t.Logf("Full container logs:\n%s", logs)
}
