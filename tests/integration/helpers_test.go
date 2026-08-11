package integration

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
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

// FuncHostProcess manages a func.exe process for a sample directory.
type FuncHostProcess struct {
	SampleDir string
	Port      int
	Env       map[string]string
	logFile   string
	logFH     *os.File
	cmd       *exec.Cmd
	t         *testing.T
}

// StartFuncHost starts func host for the given sample and waits for the
// worker to initialize before returning. func start builds the Go binary
// automatically.
func StartFuncHost(t *testing.T, sampleName string, port int, env map[string]string, initTimeout time.Duration) *FuncHostProcess {
	t.Helper()

	cleanAzuriteCheckpoints(t)

	sampleDir := filepath.Join(samplesDir(), sampleName)
	if _, err := os.Stat(sampleDir); os.IsNotExist(err) {
		t.Fatalf("sample directory not found: %s", sampleDir)
	}

	// Create log file
	logFile, err := os.CreateTemp("", fmt.Sprintf("funchost_%s_*.log", sampleName))
	if err != nil {
		t.Fatalf("failed to create log file: %v", err)
	}

	proc := &FuncHostProcess{
		SampleDir: sampleDir,
		Port:      port,
		Env:       env,
		logFile:   logFile.Name(),
		logFH:     logFile,
		t:         t,
	}

	// Prepare environment
	runEnv := os.Environ()
	for k, v := range withNativeWorkerEnvironment(env) {
		runEnv = append(runEnv, fmt.Sprintf("%s=%s", k, v))
	}

	// Start func host
	proc.cmd = exec.Command(funcExe(), "start", "--port", fmt.Sprintf("%d", port))
	proc.cmd.Dir = sampleDir
	proc.cmd.Env = runEnv
	proc.cmd.Stdout = logFile
	proc.cmd.Stderr = logFile

	if err := proc.cmd.Start(); err != nil {
		logFile.Close()
		t.Fatalf("failed to start func host: %v", err)
	}

	t.Cleanup(func() {
		proc.Stop()
	})

	// Wait for worker initialization
	if !proc.waitForPattern("Worker process started and initialized", initTimeout) {
		log := proc.ReadLog()
		lines := strings.Split(log, "\n")
		start := len(lines) - 30
		if start < 0 {
			start = 0
		}
		t.Fatalf("func host did not initialize within %v for sample %s.\nLast lines:\n%s",
			initTimeout, sampleName, strings.Join(lines[start:], "\n"))
	}

	return proc
}

// Stop kills the func host process and its entire process tree.
func (p *FuncHostProcess) Stop() {
	if p.cmd != nil && p.cmd.Process != nil {
		if runtime.GOOS == "windows" {
			// On Windows, Process.Kill() only kills the parent process.
			// Child workers (app.exe) survive and hold the port.
			// Use taskkill /T to kill the full process tree.
			exec.Command("taskkill", "/F", "/T", "/PID",
				fmt.Sprintf("%d", p.cmd.Process.Pid)).Run()
		} else {
			p.cmd.Process.Kill()
		}
		p.cmd.Wait()
	}
	if p.logFH != nil {
		p.logFH.Close()
	}
}

// ReadLog returns the current log file contents.
func (p *FuncHostProcess) ReadLog() string {
	p.logFH.Sync()
	data, err := os.ReadFile(p.logFile)
	if err != nil {
		return ""
	}
	return string(data)
}

// WaitForLog waits until a pattern appears in the log file within the given timeout.
func (p *FuncHostProcess) WaitForLog(pattern string, timeout time.Duration) bool {
	return p.waitForPattern(pattern, timeout)
}

// AssertLogContains asserts that a pattern appears in the log within timeout.
func (p *FuncHostProcess) AssertLogContains(pattern string, timeout time.Duration) {
	p.t.Helper()
	if !p.waitForPattern(pattern, timeout) {
		log := p.ReadLog()
		lines := strings.Split(log, "\n")
		start := len(lines) - 20
		if start < 0 {
			start = 0
		}
		p.t.Fatalf("pattern %q not found in log within %v.\nLast 20 lines:\n%s",
			pattern, timeout, strings.Join(lines[start:], "\n"))
	}
}

// AssertLogNotContainsError checks that no error lines appear in the log (with allowed exceptions).
func (p *FuncHostProcess) AssertLogNotContainsError(allowedPatterns ...string) {
	p.t.Helper()
	log := p.ReadLog()
	for _, line := range strings.Split(log, "\n") {
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
			p.t.Errorf("unexpected error in log: %s", line)
		}
	}
}

func (p *FuncHostProcess) waitForPattern(pattern string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		// Check if process exited
		if p.cmd.ProcessState != nil && p.cmd.ProcessState.Exited() {
			log := p.ReadLog()
			return strings.Contains(log, pattern)
		}
		log := p.ReadLog()
		if strings.Contains(log, pattern) {
			return true
		}
		time.Sleep(1 * time.Second)
	}
	return false
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
