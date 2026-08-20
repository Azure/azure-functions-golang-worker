package testrunner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/azure/azure-functions-golang-worker/tests/integration/internal/process"
	_ "github.com/microsoft/go-mssqldb"
)

type commandFunc func(context.Context, string, ...string) ([]byte, error)
type streamingCommandFunc func(context.Context, io.Writer, string, ...string) error

const cleanupTimeout = 30 * time.Second

// Emulator describes one Docker Compose service required by the integration suite.
type Emulator struct {
	Name         string
	ArtifactFile string
	waitReady    func(context.Context) error
}

// DefaultEmulators returns the complete local emulator stack used by the
// black-box integration scenarios.
func DefaultEmulators() []Emulator {
	return []Emulator{
		{Name: "azurite", ArtifactFile: "azurite.log", waitReady: waitForAzurite},
		{Name: "sqlserver", ArtifactFile: "sqlserver.log", waitReady: waitForSQLServer},
		{Name: "servicebus-emulator", ArtifactFile: "servicebus-emulator.log", waitReady: waitForServiceBus},
		{Name: "eventhub-emulator", ArtifactFile: "eventhub-emulator.log", waitReady: waitForEventHub},
		{Name: "cosmosdb-emulator", ArtifactFile: "cosmosdb-emulator.log", waitReady: waitForCosmosDB},
	}
}

// Runner owns the shared integration-test lifecycle around the Go tests:
// prerequisite validation, emulator startup/readiness, diagnostics, and cleanup.
type Runner struct {
	ComposeFile             string
	ArtifactDir             string
	TestPattern             string
	FuncExe                 string
	MinimumCoreToolsVersion string
	Emulators               []Emulator

	command     commandFunc
	testCommand streamingCommandFunc
	terminate   func(int) error
	output      io.Writer
}

func (r Runner) Run(ctx context.Context) (runErr error) {
	if len(r.Emulators) == 0 {
		return errors.New("no emulators configured")
	}

	// Unit tests inject fake command and readiness functions so they can verify
	// orchestration behavior without starting external processes.
	command := r.command
	if command == nil {
		command = runCommand
	}
	testCommand := r.testCommand
	if testCommand == nil {
		testCommand = runStreamingCommand
	}
	outputWriter := r.output
	if outputWriter == nil {
		outputWriter = os.Stdout
	}
	emulators := r.Emulators
	if err := os.MkdirAll(r.ArtifactDir, 0o755); err != nil {
		return fmt.Errorf("create artifact directory: %w", err)
	}
	if err := clearHostPIDFiles(r.ArtifactDir); err != nil {
		return fmt.Errorf("clear stale host process records: %w", err)
	}

	// Tool installation belongs to the caller (local setup or the pipeline).
	// The runner only verifies that the expected tools are available.
	funcExe := r.FuncExe
	if funcExe == "" {
		funcExe = "func"
	}
	output, err := command(ctx, funcExe, "--version")
	if err != nil {
		return fmt.Errorf("run Core Tools %q: %w\n%s", funcExe, err, output)
	}
	if r.MinimumCoreToolsVersion != "" {
		if err := validateMinimumVersion(string(output), r.MinimumCoreToolsVersion); err != nil {
			return err
		}
	}

	output, err = command(ctx, "docker", "compose", "version")
	if err != nil {
		return fmt.Errorf("run Docker Compose: %w\n%s", err, output)
	}

	composeArgs := []string{"compose", "-f", r.ComposeFile}
	owned := make(map[string]bool, len(emulators))
	var startServices []string
	for _, emulator := range emulators {
		output, err = command(ctx, "docker", append(composeArgs, "ps", "-a", "-q", emulator.Name)...)
		if err != nil {
			return fmt.Errorf("inspect %s container: %w\n%s", emulator.Name, err, output)
		}
		owned[emulator.Name] = strings.TrimSpace(string(output)) == ""

		output, err = command(ctx, "docker", append(composeArgs, "ps", "--status", "running", "-q", emulator.Name)...)
		if err != nil {
			return fmt.Errorf("inspect %s running state: %w\n%s", emulator.Name, err, output)
		}
		if strings.TrimSpace(string(output)) == "" {
			startServices = append(startServices, emulator.Name)
		}
	}

	// Register diagnostics and cleanup before startup. Docker Compose can return
	// an error after creating a container, so deferring later could leak it.
	// The original failure remains visible if cleanup also reports a problem.
	defer func() {
		var cleanupErrs []error

		terminate := r.terminate
		if terminate == nil {
			terminate = process.TerminateTreePID
		}
		if err := terminateRegisteredHosts(r.ArtifactDir, terminate); err != nil {
			cleanupErrs = append(cleanupErrs, err)
		}

		for _, emulator := range emulators {
			logCtx, cancelLogs := context.WithTimeout(context.Background(), cleanupTimeout)
			logOutput, logErr := command(logCtx, "docker",
				append(composeArgs, "logs", "--no-color", emulator.Name)...)
			cancelLogs()
			artifactFile := emulator.ArtifactFile
			if artifactFile == "" {
				artifactFile = emulator.Name + ".log"
			}

			if writeErr := os.WriteFile(filepath.Join(r.ArtifactDir, artifactFile), logOutput, 0o600); writeErr != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("write %s log: %w", emulator.Name, writeErr))
			}
			if logErr != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("collect %s log: %w", emulator.Name, logErr))
			}
		}

		var removeServices []string
		for i := len(emulators) - 1; i >= 0; i-- {
			if owned[emulators[i].Name] {
				removeServices = append(removeServices, emulators[i].Name)
			}
		}
		if len(removeServices) > 0 {
			removeCtx, cancelRemove := context.WithTimeout(context.Background(), cleanupTimeout)
			cleanupOutput, cleanupErr := command(removeCtx, "docker",
				append(append(composeArgs, "rm", "-f", "-s"), removeServices...)...)
			cancelRemove()
			if cleanupErr != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("remove integration emulators: %w\n%s", cleanupErr, cleanupOutput))
			}
		}
		runErr = errors.Join(append([]error{runErr}, cleanupErrs...)...)
	}()

	// Reuse developer-owned containers. Only containers absent at runner startup
	// are considered runner-owned and eligible for removal.
	if len(startServices) > 0 {
		output, err = command(ctx, "docker", append(append(composeArgs, "up", "-d"), startServices...)...)
		if err != nil {
			return fmt.Errorf("start integration emulators: %w\n%s", err, output)
		}
	}

	// Do not start Core Tools until the emulator responds at the service layer.
	// A running container or open TCP port alone does not prove readiness.
	for _, emulator := range emulators {
		if emulator.waitReady == nil {
			continue
		}
		if err := emulator.waitReady(ctx); err != nil {
			return fmt.Errorf("wait for %s readiness: %w", emulator.Name, err)
		}
	}

	// Run the selected tests consistently for local and Azure DevOps runs.
	testArgs := []string{"test", "-v", "-count=1", "-run", r.TestPattern, "."}
	testLogPath := filepath.Join(r.ArtifactDir, "go-test.log")
	testLog, err := os.OpenFile(testLogPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create Go test log: %w", err)
	}
	// Show progress live and save the same output for troubleshooting.
	testErr := testCommand(ctx, io.MultiWriter(outputWriter, testLog), "go", testArgs...)
	closeErr := testLog.Close()
	if testErr != nil {
		runErr := fmt.Errorf("run integration tests: %w (output: %s)", testErr, testLogPath)
		if closeErr != nil {
			return errors.Join(runErr, fmt.Errorf("close Go test log: %w", closeErr))
		}
		return runErr
	}
	if closeErr != nil {
		return fmt.Errorf("close Go test log: %w", closeErr)
	}
	return nil
}

func clearHostPIDFiles(artifactDir string) error {
	return filepath.WalkDir(artifactDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && entry.Name() == process.PIDFileName {
			return os.Remove(path)
		}
		return nil
	})
}

func terminateRegisteredHosts(artifactDir string, terminate func(int) error) error {
	var cleanupErrs []error
	walkErr := filepath.WalkDir(artifactDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != process.PIDFileName {
			return nil
		}

		content, err := os.ReadFile(path)
		if err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("read host process record %s: %w", path, err))
			return nil
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(content)))
		if err != nil || pid <= 0 {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("parse host process record %s: invalid pid %q", path, content))
			return nil
		}
		if err := terminate(pid); err != nil {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("terminate orphaned host process %d: %w", pid, err))
			return nil
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("remove host process record %s: %w", path, err))
		}
		return nil
	})
	if walkErr != nil {
		cleanupErrs = append(cleanupErrs, fmt.Errorf("scan host process records: %w", walkErr))
	}
	return errors.Join(cleanupErrs...)
}

func runCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	return cmd.CombinedOutput()
}

func runStreamingCommand(ctx context.Context, output io.Writer, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = output
	cmd.Stderr = output
	process.Configure(cmd)
	// If the suite is cancelled, stop the test and anything it started.
	cmd.Cancel = func() error {
		return process.TerminateTree(cmd)
	}
	return cmd.Run()
}

func validateMinimumVersion(actualOutput, minimumVersion string) error {
	actual, err := parseCoreToolsVersionOutput(actualOutput)
	if err != nil {
		return fmt.Errorf("invalid Core Tools version output %q: %w", strings.TrimSpace(actualOutput), err)
	}
	minimum, err := parseCoreToolsVersionOutput(minimumVersion)
	if err != nil {
		return fmt.Errorf("invalid minimum Core Tools version %q: %w", strings.TrimSpace(minimumVersion), err)
	}
	if compareVersionComponents(actual, minimum) < 0 {
		return fmt.Errorf("Core Tools version is %s, require %s or later", strings.TrimSpace(actualOutput), strings.TrimSpace(minimumVersion))
	}
	return nil
}

func parseCoreToolsVersionOutput(output string) ([3]uint64, error) {
	trimmed := strings.TrimSpace(output)
	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return [3]uint64{}, fmt.Errorf("empty output")
	}
	return parseCoreToolsVersionToken(fields[0])
}

func parseCoreToolsVersionToken(token string) ([3]uint64, error) {
	token = strings.TrimPrefix(token, "v")
	parts := strings.SplitN(token, ".", 3)
	if len(parts) != 3 {
		return [3]uint64{}, fmt.Errorf("want at least 3 numeric components")
	}

	major, err := parseNumericComponent(parts[0])
	if err != nil {
		return [3]uint64{}, err
	}
	minor, err := parseNumericComponent(parts[1])
	if err != nil {
		return [3]uint64{}, err
	}
	patch, err := parsePatchComponent(parts[2])
	if err != nil {
		return [3]uint64{}, err
	}
	return [3]uint64{major, minor, patch}, nil
}

func parseNumericComponent(part string) (uint64, error) {
	if part == "" {
		return 0, fmt.Errorf("want at least 3 numeric components")
	}
	for i := 0; i < len(part); i++ {
		if part[i] < '0' || part[i] > '9' {
			return 0, fmt.Errorf("want at least 3 numeric components")
		}
	}
	return strconv.ParseUint(part, 10, 64)
}

func parsePatchComponent(part string) (uint64, error) {
	if part == "" {
		return 0, fmt.Errorf("want at least 3 numeric components")
	}
	i := 0
	for i < len(part) && part[i] >= '0' && part[i] <= '9' {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("want at least 3 numeric components")
	}
	if i < len(part) && part[i] != '-' && part[i] != '+' {
		return 0, fmt.Errorf("want at least 3 numeric components")
	}
	return strconv.ParseUint(part[:i], 10, 64)
}

func compareVersionComponents(a, b [3]uint64) int {
	for i := 0; i < 3; i++ {
		switch {
		case a[i] < b[i]:
			return -1
		case a[i] > b[i]:
			return 1
		}
	}
	return 0
}

// waitForAzurite verifies the services used by AzureWebJobsStorage. HTTP error
// responses below 500 are accepted because they prove the emulator processed
// the request even when the unauthenticated probe itself is rejected.
func waitForAzurite(ctx context.Context) error {
	client := &http.Client{Timeout: 2 * time.Second}
	for _, endpoint := range []string{
		"http://127.0.0.1:10000/devstoreaccount1?comp=list",
		"http://127.0.0.1:10001/devstoreaccount1?comp=list",
	} {
		if err := waitHTTPReady(ctx, client, endpoint, 250*time.Millisecond); err != nil {
			return err
		}
	}
	return nil
}

func waitForSQLServer(ctx context.Context) error {
	password := os.Getenv("SQL_TEST_PASSWORD")
	if password == "" {
		password = "StrongP@ssw0rd!"
	}
	connectionURL := &url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword("sa", password),
		Host:   "127.0.0.1:1433",
	}
	query := connectionURL.Query()
	query.Set("database", "master")
	query.Set("encrypt", "disable")
	query.Set("TrustServerCertificate", "true")
	connectionURL.RawQuery = query.Encode()

	db, err := sql.Open("sqlserver", connectionURL.String())
	if err != nil {
		return err
	}
	defer db.Close()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		lastErr = db.PingContext(pingCtx)
		cancel()
		if lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}

func waitForServiceBus(ctx context.Context) error {
	return waitTCPReady(ctx, "127.0.0.1:5672", 250*time.Millisecond)
}

func waitForEventHub(ctx context.Context) error {
	return waitTCPReady(ctx, "127.0.0.2:5672", 250*time.Millisecond)
}

func waitForCosmosDB(ctx context.Context) error {
	client := &http.Client{Timeout: 2 * time.Second}
	return waitHTTPReady(ctx, client, "http://127.0.0.1:8081/", 500*time.Millisecond)
}

func waitTCPReady(ctx context.Context, address string, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	dialer := net.Dialer{Timeout: 2 * time.Second}
	var lastErr error
	for {
		conn, err := dialer.DialContext(ctx, "tcp", address)
		if err == nil {
			conn.Close()
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}

// waitHTTPReady polls until the endpoint can process requests or the caller's
// deadline expires. lastErr is joined with the context error for diagnostics.
func waitHTTPReady(ctx context.Context, client *http.Client, endpoint string, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastErr error
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return fmt.Errorf("create readiness request: %w", err)
		}
		response, err := client.Do(request)
		if err == nil {
			response.Body.Close()
			if response.StatusCode < http.StatusInternalServerError {
				return nil
			}
			lastErr = fmt.Errorf("status %s", response.Status)
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}
