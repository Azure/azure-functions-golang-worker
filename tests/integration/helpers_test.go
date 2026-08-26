package integration

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/azure/azure-functions-golang-worker/tests/integration/internal/testhost"
)

// repoRoot returns the absolute path to the repository root.
func repoRoot() string {
	// This file lives at tests/integration/helpers_test.go
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..")
}

// samplesDir returns the absolute path to the samples directory.
func samplesDir() string {
	return filepath.Join(repoRoot(), "samples")
}

func integrationTestDataDir() string {
	return filepath.Join(repoRoot(), "tests", "integration", "testdata")
}

// funcExe returns the path to the func CLI executable.
func funcExe() string {
	if v := os.Getenv("FUNC_EXE"); v != "" {
		return v
	}
	return "func"
}

func withNativeWorkerEnvironment(env map[string]string) map[string]string {
	nativeEnv := make(map[string]string, len(env)+2)
	for key, value := range env {
		nativeEnv[key] = value
	}
	nativeEnv["FUNCTIONS_WORKER_RUNTIME"] = "native"
	nativeEnv["FUNCTIONS_CLI_NATIVE_LANGUAGE"] = "go"
	return nativeEnv
}

// requireEmulator checks that a TCP endpoint is reachable, and fails the test if not.
func requireEmulator(t *testing.T, name, addr string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("%s is not running at %s — start emulators with: docker compose -f tests/emulators/docker-compose.yml up -d", name, addr)
	}
	conn.Close()
}

func requireAzurite(t *testing.T) { t.Helper(); requireEmulator(t, "azurite", "127.0.0.1:10000") }
func requireCosmosDB(t *testing.T) {
	t.Helper()
	requireEmulator(t, "cosmosdb-emulator", "127.0.0.1:8081")
}
func requireServiceBus(t *testing.T) {
	t.Helper()
	requireEmulator(t, "servicebus-emulator", "127.0.0.1:5672")
}
func requireEventHub(t *testing.T) {
	t.Helper()
	requireEmulator(t, "eventhub-emulator", "127.0.0.2:5672")
}
func requireSQLServer(t *testing.T) {
	t.Helper()
	requireEmulator(t, "sqlserver", "127.0.0.1:1433")
}

// cleanAzuriteCheckpoints deletes stale checkpoint and receipt containers from
// Azurite. The Azure Functions host stores trigger state (blob receipts,
// Event Hub partition checkpoints) in well-known containers. If these persist
// between test runs, triggers may skip events they consider already-processed.
func cleanAzuriteCheckpoints(t *testing.T) {
	t.Helper()
	client, err := azblob.NewClientFromConnectionString(azuriteConnStr, nil)
	if err != nil {
		t.Logf("skipping azurite checkpoint cleanup: %v", err)
		return
	}
	ctx := context.Background()
	for _, name := range []string{"azure-webjobs-hosts", "azure-webjobs-eventhub"} {
		_, _ = client.DeleteContainer(ctx, name, nil)
	}
}

func startSampleHost(t *testing.T, sampleName string, env map[string]string, initTimeout time.Duration) testhost.Host {
	t.Helper()
	return startFunctionHost(t, sampleName, filepath.Join(samplesDir(), sampleName), env, initTimeout)
}

func startTestDataHost(t *testing.T, appName string, env map[string]string, initTimeout time.Duration) testhost.Host {
	t.Helper()
	return startFunctionHost(t, appName, filepath.Join(integrationTestDataDir(), appName), env, initTimeout)
}

func startFunctionHost(
	t *testing.T,
	appName string,
	appDir string,
	env map[string]string,
	initTimeout time.Duration,
) testhost.Host {
	t.Helper()
	cleanAzuriteCheckpoints(t)

	host, err := testhost.Start(context.Background(), testhost.Config{
		SampleDir:   appDir,
		FuncExe:     funcExe(),
		Environment: env,
		ArtifactDir: filepath.Join("artifacts", t.Name()),
		InitTimeout: initTimeout,
	})
	if err != nil {
		t.Fatalf("start %s function host: %v", appName, err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := host.Stop(stopCtx); err != nil {
			t.Errorf("stop %s function host: %v", appName, err)
		}
	})
	return host
}

func assertHostLogContains(t *testing.T, host testhost.Host, pattern string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := host.WaitForLog(ctx, pattern); err != nil {
		t.Fatalf("wait for host log pattern %q: %v", pattern, err)
	}
}

func hostLogContains(host testhost.Host, pattern string, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return host.WaitForLog(ctx, pattern) == nil
}

func assertHostLogNotContainsError(t *testing.T, host testhost.Host, allowedPatterns ...string) {
	t.Helper()

	logBytes, err := os.ReadFile(host.LogPath())
	if err != nil {
		t.Fatalf("read host log: %v", err)
	}
	for _, line := range strings.Split(string(logBytes), "\n") {
		lower := strings.ToLower(line)
		if !strings.Contains(lower, "error") {
			continue
		}
		allowed := false
		for _, pattern := range allowedPatterns {
			if strings.Contains(line, pattern) {
				allowed = true
				break
			}
		}
		if !allowed {
			t.Errorf("unexpected error in log: %s", line)
		}
	}
}

// readAll is a test helper to read the full body of an io.ReadCloser.
func readAll(t *testing.T, body io.ReadCloser) []byte {
	t.Helper()
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	return data
}
