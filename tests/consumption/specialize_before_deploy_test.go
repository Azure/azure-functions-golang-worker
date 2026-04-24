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
			"AzureWebJobsStorage":         connStr,
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
//  5. New pod starts fresh → specialization → binary exists from FUSE mount
//  6. /api/hello returns 200
//
// We simulate the pod recycle (step 4-5) with docker restart: the container
// filesystem state (deployed binary) is preserved, but all processes restart
// fresh — just like a new pod with content already mounted.
func TestSpecializeBeforeDeployFullFlow(t *testing.T) {
	requireDocker(t)

	connStr := azuriteConnString(t)
	zipPath := buildSampleZipMinimal(t, "httpTrigger")

	// Start in placeholder mode with no app pre-baked.
	fc := buildAndStartFlex(t,
		"Dockerfile.flex-test-specialize-before-deploy",
		"goworker-specialize-before-deploy-full:latest")
	fc.waitForPing(90 * time.Second)

	// --- Phase 1: Specialize without binary ---
	t.Log("Phase 1: Specializing without app deployed...")
	fc.specialize(map[string]string{
		"AzureWebJobsStorage": connStr,
	})

	time.Sleep(15 * time.Second)

	// Verify container is healthy (proxy didn't crash-loop).
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", fc.id).CombinedOutput()
	if err != nil || strings.TrimSpace(string(out)) != "true" {
		t.Fatalf("container is not running after specialize without app\nrunning=%s\nlogs:\n%s",
			strings.TrimSpace(string(out)), fc.logs())
	}

	logs := fc.logs()
	if !strings.Contains(logs, "running as no-op worker") {
		t.Fatalf("Proxy did not enter no-op mode during specialization\nlogs:\n%s", logs)
	}
	t.Log("Phase 1 PASSED: Container healthy, proxy in no-op mode")

	// --- Phase 2: Deploy binary ---
	t.Log("Phase 2: Deploying app...")
	fc.deployApp(zipPath)
	out, _ = exec.Command("docker", "exec", fc.id, "ls", "-la", "/home/site/wwwroot/").CombinedOutput()
	t.Logf("wwwroot after deploy:\n%s", out)

	// --- Phase 3: Simulate pod recycle (docker restart) ---
	// In production, Kudu calls removeworker/allStandard to kill all pods.
	// New pods start fresh with content already on the FUSE mount.
	// docker restart preserves filesystem state but restarts all processes.
	t.Log("Phase 3: Restarting container (simulating pod recycle)...")
	fc.restartContainer()
	fc.waitForPing(90 * time.Second)

	// Specialize the restarted container (like a new pod being assigned).
	t.Log("Specializing restarted container (binary now exists)...")
	fc.specialize(map[string]string{
		"AzureWebJobsStorage": connStr,
	})

	// --- Phase 4: Verify function is callable ---
	status, body := fc.sendRequestWithTimeout("GET", "/api/hello", 60*time.Second)
	if status != 200 {
		t.Fatalf("expected 200 from /api/hello, got %d\nlogs:\n%s", status, fc.logs())
	}
	if string(body) != "Hello from Go Worker!" {
		t.Fatalf("expected 'Hello from Go Worker!', got %q", string(body))
	}
	t.Log("Phase 4 PASSED: /api/hello returned 200 after deploy + pod recycle")
}

// TestSpecializeWithNonExecutableBinary verifies that the proxy catches a
// missing execute permission on the app binary and reports a clear error
// instead of a bare "exited with code 1" in App Insights.
//
// This covers the case where a user deploys a binary built on Windows or
// uploads a zip that doesn't preserve Unix permissions.
func TestSpecializeWithNonExecutableBinary(t *testing.T) {
	requireDocker(t)

	connStr := azuriteConnString(t)
	zipPath := buildSampleZipMinimal(t, "httpTrigger")

	// Start in placeholder mode with no app pre-baked.
	fc := buildAndStartFlex(t,
		"Dockerfile.flex-test-specialize-before-deploy",
		"goworker-non-executable-test:latest")
	fc.waitForPing(90 * time.Second)

	// Deploy the app but WITHOUT execute permissions.
	// Use deployApp (which does chmod +x), then remove the execute bit.
	fc.deployApp(zipPath)
	out, err := exec.Command("docker", "exec", fc.id, "chmod", "-x", "/home/site/wwwroot/app").CombinedOutput()
	if err != nil {
		t.Fatalf("failed to remove execute permission: %v\n%s", err, out)
	}
	out, _ = exec.Command("docker", "exec", fc.id, "ls", "-la", "/home/site/wwwroot/app").CombinedOutput()
	t.Logf("Binary permissions after chmod -x: %s", strings.TrimSpace(string(out)))

	// Specialize — the proxy should detect the non-executable binary and
	// report a clear error via FERR response instead of crashing.
	t.Log("Specializing with non-executable binary...")
	fc.specialize(map[string]string{
		"AzureWebJobsStorage": connStr,
	})

	// Wait for the host to process the specialization.
	time.Sleep(15 * time.Second)

	logs := fc.logs()

	// Verify the proxy caught the permission issue.
	if strings.Contains(logs, "not executable") && strings.Contains(logs, "chmod +x") {
		t.Log("CONFIRMED: Proxy detected non-executable binary and reported clear error")
	} else {
		t.Errorf("Expected 'not executable' and 'chmod +x' in logs but not found")
	}

	// Verify the proxy did NOT crash with a bare error.
	if strings.Contains(logs, "Failed to start child worker") {
		t.Errorf("Proxy crashed instead of reporting permission error")
	} else {
		t.Log("CONFIRMED: No crash — error reported through FERR response")
	}

	// Container should still be running (proxy stayed alive).
	out, err = exec.Command("docker", "inspect", "-f", "{{.State.Running}}", fc.id).CombinedOutput()
	if err != nil || strings.TrimSpace(string(out)) != "true" {
		t.Fatalf("container is not running\nrunning=%s\nlogs:\n%s",
			strings.TrimSpace(string(out)), logs)
	}
	t.Log("CONFIRMED: Container still running after permission error")
}
