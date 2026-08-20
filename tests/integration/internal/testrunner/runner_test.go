package testrunner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/azure/azure-functions-golang-worker/tests/integration/internal/process"
)

func TestWaitHTTPReadyPollsUntilServiceResponds(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		if attempts < 3 {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waitHTTPReady(ctx, server.Client(), server.URL, 10*time.Millisecond); err != nil {
		t.Fatalf("waitHTTPReady() error = %v", err)
	}
	if attempts != 3 {
		t.Fatalf("readiness attempts = %d, want 3", attempts)
	}
}

func TestRunRequiresEmulatorConfiguration(t *testing.T) {
	err := (Runner{}).Run(context.Background())
	if err == nil || err.Error() != "no emulators configured" {
		t.Fatalf("Run() error = %v, want no emulators configured", err)
	}
}

// TestRunNoExistingContainer covers: two ps queries, starts Azurite, runs tests,
// captures logs, and removes the container it created.
func TestRunNoExistingContainer(t *testing.T) {
	artifactDir := t.TempDir()
	var commands []string
	var streamedOutput bytes.Buffer
	command := emulatorLifecycleCommand(&commands, map[string]emulatorFixture{
		"azurite": {log: "azurite diagnostics"},
	})
	testCommand := successfulTestCommand(&commands, "PASS\n")

	runner := Runner{
		ComposeFile:             filepath.Join("tests", "emulators", "docker-compose.yml"),
		ArtifactDir:             artifactDir,
		TestPattern:             "^TestHttpTriggerGet$",
		FuncExe:                 "func",
		MinimumCoreToolsVersion: "4.12.0",
		Emulators:               testAzuriteEmulators(),
		command:                 command,
		testCommand:             testCommand,
		output:                  &streamedOutput,
	}
	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	cf := filepath.Join("tests", "emulators", "docker-compose.yml")
	wantCommands := []string{
		"func --version",
		"docker compose version",
		"docker compose -f " + cf + " ps -a -q azurite",
		"docker compose -f " + cf + " ps --status running -q azurite",
		"docker compose -f " + cf + " up -d azurite",
		"go test -v -count=1 -run ^TestHttpTriggerGet$ .",
		"docker compose -f " + cf + " logs --no-color azurite",
		"docker compose -f " + cf + " rm -f -s azurite",
	}
	if strings.Join(commands, "\n") != strings.Join(wantCommands, "\n") {
		t.Fatalf("commands:\n%s\nwant:\n%s", strings.Join(commands, "\n"), strings.Join(wantCommands, "\n"))
	}

	assertFileContains(t, filepath.Join(artifactDir, "go-test.log"), "PASS")
	assertFileContains(t, filepath.Join(artifactDir, "azurite.log"), "azurite diagnostics")
	if !strings.Contains(streamedOutput.String(), "PASS") {
		t.Fatalf("streamed output does not contain test output:\n%s", streamedOutput.String())
	}
}

func TestRunMultipleEmulatorsPreservesExistingContainers(t *testing.T) {
	artifactDir := t.TempDir()
	var commands []string
	var readiness []string
	command := emulatorLifecycleCommand(&commands, map[string]emulatorFixture{
		"azurite":             {log: "azurite log"},
		"sqlserver":           {containerID: "sql-id", log: "sql log"},
		"servicebus-emulator": {containerID: "sb-id", running: true, log: "service bus log"},
	})
	ready := func(name string) func(context.Context) error {
		return func(context.Context) error {
			readiness = append(readiness, name)
			return nil
		}
	}

	runner := Runner{
		ComposeFile:             "compose.yml",
		ArtifactDir:             artifactDir,
		TestPattern:             "^TestIntegration$",
		FuncExe:                 "func",
		MinimumCoreToolsVersion: "4.12.0",
		Emulators: []Emulator{
			{Name: "azurite", ArtifactFile: "azurite.log", waitReady: ready("azurite")},
			{Name: "sqlserver", ArtifactFile: "sqlserver.log", waitReady: ready("sqlserver")},
			{Name: "servicebus-emulator", ArtifactFile: "servicebus.log", waitReady: ready("servicebus-emulator")},
		},
		command:     command,
		testCommand: successfulTestCommand(&commands, "PASS\n"),
		output:      io.Discard,
	}
	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := strings.Join(readiness, ","); got != "azurite,sqlserver,servicebus-emulator" {
		t.Fatalf("readiness order = %q", got)
	}
	assertCommandExecuted(t, commands, "docker compose -f compose.yml up -d azurite sqlserver")
	assertCommandExecuted(t, commands, "docker compose -f compose.yml rm -f -s azurite")
	assertFileContains(t, filepath.Join(artifactDir, "azurite.log"), "azurite log")
	assertFileContains(t, filepath.Join(artifactDir, "sqlserver.log"), "sql log")
	assertFileContains(t, filepath.Join(artifactDir, "servicebus.log"), "service bus log")
}

// TestRunExistingRunningContainer covers: two ps queries, no startup, no removal.
func TestRunExistingRunningContainer(t *testing.T) {
	var commands []string
	command := emulatorLifecycleCommand(&commands, map[string]emulatorFixture{
		"azurite": {containerID: "abc123", running: true},
	})

	runner := Runner{
		ComposeFile:             "compose.yml",
		ArtifactDir:             t.TempDir(),
		TestPattern:             "^TestHttpTriggerGet$",
		FuncExe:                 "func",
		MinimumCoreToolsVersion: "4.12.0",
		Emulators:               testAzuriteEmulators(),
		command:                 command,
		testCommand:             successfulTestCommand(&commands, "PASS\n"),
		output:                  io.Discard,
	}
	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for _, c := range commands {
		if strings.Contains(c, "up -d") || strings.Contains(c, "rm -f") {
			t.Fatalf("runner changed lifecycle of pre-existing running container: %s", c)
		}
	}
}

// TestRunExistingStoppedContainer covers: two ps queries, starts container, does NOT remove it.
func TestRunExistingStoppedContainer(t *testing.T) {
	var commands []string
	command := emulatorLifecycleCommand(&commands, map[string]emulatorFixture{
		"azurite": {containerID: "abc123"},
	})

	runner := Runner{
		ComposeFile:             "compose.yml",
		ArtifactDir:             t.TempDir(),
		TestPattern:             "^TestHttpTriggerGet$",
		FuncExe:                 "func",
		MinimumCoreToolsVersion: "4.12.0",
		Emulators:               testAzuriteEmulators(),
		command:                 command,
		testCommand:             successfulTestCommand(&commands, "PASS\n"),
		output:                  io.Discard,
	}
	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	startedAzurite := false
	for _, c := range commands {
		if strings.Contains(c, "up -d azurite") {
			startedAzurite = true
		}
		if strings.Contains(c, "rm -f") {
			t.Fatalf("runner removed pre-existing stopped container: %s", c)
		}
	}
	if !startedAzurite {
		t.Fatal("runner did not start the stopped container")
	}
}

func TestValidateMinimumVersion(t *testing.T) {
	tests := []struct {
		name           string
		actual         string
		minimumVersion string
		wantErrors     []string
	}{
		{name: "equal version", actual: "4.12.0", minimumVersion: "4.12.0"},
		{name: "newer patch", actual: "4.12.1", minimumVersion: "4.12.0"},
		{name: "newer minor", actual: "4.13.0", minimumVersion: "4.12.0"},
		{name: "leading v", actual: "v4.12.0", minimumVersion: "4.12.0"},
		{name: "suffix ignored", actual: "4.12.0-preview.1", minimumVersion: "4.12.0"},
		{
			name:           "extra core component",
			actual:         "4.12.0.1",
			minimumVersion: "4.12.0",
			wantErrors:     []string{`"4.12.0.1"`},
		},
		{
			name:           "older version",
			actual:         "4.11.9",
			minimumVersion: "4.12.0",
			wantErrors:     []string{"Core Tools version is 4.11.9", "require 4.12.0 or later"},
		},
		{
			name:           "malformed output",
			actual:         "garbage output",
			minimumVersion: "4.12.0",
			wantErrors:     []string{`"garbage output"`},
		},
		{
			name:           "empty output",
			actual:         "   \n",
			minimumVersion: "4.12.0",
			wantErrors:     []string{`""`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateMinimumVersion(tt.actual, tt.minimumVersion)
			if len(tt.wantErrors) == 0 {
				if err != nil {
					t.Fatalf("validateMinimumVersion(%q) error = %v", tt.actual, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateMinimumVersion(%q) error = nil, want error containing %q", tt.actual, tt.wantErrors)
			}
			for _, want := range tt.wantErrors {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("validateMinimumVersion(%q) error = %q, want substring %q", tt.actual, err.Error(), want)
				}
			}
		})
	}
}

func TestRunRejectsUnexpectedCoreToolsVersion(t *testing.T) {
	command := func(_ context.Context, name string, args ...string) ([]byte, error) {
		call := strings.Join(append([]string{name}, args...), " ")
		if call == "func --version" {
			return []byte("4.11.0\n"), nil
		}
		return nil, errors.New("unexpected command: " + call)
	}

	err := (Runner{
		ArtifactDir:             t.TempDir(),
		FuncExe:                 "func",
		MinimumCoreToolsVersion: "4.12.0",
		Emulators:               testAzuriteEmulators(),
		command:                 command,
	}).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "4.11.0") || !strings.Contains(err.Error(), "require 4.12.0 or later") {
		t.Fatalf("Run() error = %v, want Core Tools version mismatch", err)
	}
}

func TestRunAllowsNewerCoreToolsVersionBeforeDockerValidation(t *testing.T) {
	var commands []string
	command := func(_ context.Context, name string, args ...string) ([]byte, error) {
		call := strings.Join(append([]string{name}, args...), " ")
		commands = append(commands, call)
		switch call {
		case "func --version":
			return []byte("4.13.0\n"), nil
		case "docker compose version":
			return []byte("docker validation failed"), errors.New("docker validation failed")
		default:
			return nil, errors.New("unexpected command: " + call)
		}
	}

	err := (Runner{
		ArtifactDir:             t.TempDir(),
		FuncExe:                 "func",
		MinimumCoreToolsVersion: "4.12.0",
		Emulators:               testAzuriteEmulators(),
		command:                 command,
	}).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "docker validation failed") {
		t.Fatalf("Run() error = %v, want Docker validation failure", err)
	}
	wantCommands := []string{"func --version", "docker compose version"}
	if strings.Join(commands, "\n") != strings.Join(wantCommands, "\n") {
		t.Fatalf("commands:\n%s\nwant:\n%s", strings.Join(commands, "\n"), strings.Join(wantCommands, "\n"))
	}
}

func TestRunCleansAzuriteAfterPartialStartupFailure(t *testing.T) {
	artifactDir := t.TempDir()
	var commands []string
	command := emulatorLifecycleCommand(&commands, map[string]emulatorFixture{
		"azurite": {
			log:         "partial startup diagnostics",
			startOutput: "container created before startup failure",
			startErr:    errors.New("compose startup failed"),
		},
	})

	err := (Runner{
		ComposeFile:             "compose.yml",
		ArtifactDir:             artifactDir,
		TestPattern:             "^TestHttpTriggerGet$",
		FuncExe:                 "func",
		MinimumCoreToolsVersion: "4.12.0",
		Emulators:               testAzuriteEmulators(),
		command:                 command,
	}).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "compose startup failed") {
		t.Fatalf("Run() error = %v, want original startup failure", err)
	}

	gotCommands := strings.Join(commands, "\n")
	if !strings.Contains(gotCommands, "logs --no-color azurite") ||
		!strings.Contains(gotCommands, "rm -f -s azurite") {
		t.Fatalf("partial startup did not collect logs and clean up:\n%s", gotCommands)
	}
	assertFileContains(t, filepath.Join(artifactDir, "azurite.log"), "partial startup diagnostics")
}

func TestRunReapsRegisteredHostAfterTestProcessFailure(t *testing.T) {
	artifactDir := t.TempDir()
	var commands []string
	command := emulatorLifecycleCommand(&commands, map[string]emulatorFixture{
		"azurite": {log: "azurite diagnostics"},
	})
	testCommand := func(_ context.Context, _ io.Writer, _ string, _ ...string) error {
		scenarioDir := filepath.Join(artifactDir, "TestScenario")
		if err := os.MkdirAll(scenarioDir, 0o755); err != nil {
			return err
		}
		return errors.Join(
			os.WriteFile(filepath.Join(scenarioDir, process.PIDFileName), []byte("123"), 0o600),
			errors.New("test process terminated"),
		)
	}
	var terminatedPIDs []int

	err := (Runner{
		ComposeFile:             "compose.yml",
		ArtifactDir:             artifactDir,
		TestPattern:             "^TestScenario$",
		FuncExe:                 "func",
		MinimumCoreToolsVersion: "4.12.0",
		Emulators:               testAzuriteEmulators(),
		command:                 command,
		testCommand:             testCommand,
		terminate: func(pid int) error {
			terminatedPIDs = append(terminatedPIDs, pid)
			return nil
		},
	}).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "test process terminated") {
		t.Fatalf("Run() error = %v, want test process failure", err)
	}
	if !slices.Equal(terminatedPIDs, []int{123}) {
		t.Fatalf("terminated pids = %v, want [123]", terminatedPIDs)
	}
}

func TestClearHostPIDFilesRemovesOnlyProcessRecords(t *testing.T) {
	artifactDir := t.TempDir()
	nestedDir := filepath.Join(artifactDir, "TestScenario")
	if err := os.MkdirAll(nestedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	pidPath := filepath.Join(nestedDir, process.PIDFileName)
	logPath := filepath.Join(nestedDir, "host.log")
	if err := os.WriteFile(pidPath, []byte("123"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, []byte("diagnostics"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := clearHostPIDFiles(artifactDir); err != nil {
		t.Fatalf("clearHostPIDFiles() error = %v", err)
	}
	if _, err := os.Stat(pidPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("process record still exists: %v", err)
	}
	assertFileContains(t, logPath, "diagnostics")
}

func TestTerminateRegisteredHostsTerminatesEachRecordedProcess(t *testing.T) {
	artifactDir := t.TempDir()
	var wantPIDs []int
	for index, pid := range []int{123, 456} {
		scenarioDir := filepath.Join(artifactDir, fmt.Sprintf("scenario-%d", index))
		if err := os.MkdirAll(scenarioDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(
			filepath.Join(scenarioDir, process.PIDFileName),
			[]byte(strconv.Itoa(pid)),
			0o600,
		); err != nil {
			t.Fatal(err)
		}
		wantPIDs = append(wantPIDs, pid)
	}

	var gotPIDs []int
	err := terminateRegisteredHosts(artifactDir, func(pid int) error {
		gotPIDs = append(gotPIDs, pid)
		return nil
	})
	if err != nil {
		t.Fatalf("terminateRegisteredHosts() error = %v", err)
	}
	slices.Sort(gotPIDs)
	if !slices.Equal(gotPIDs, wantPIDs) {
		t.Fatalf("terminated pids = %v, want %v", gotPIDs, wantPIDs)
	}

	if err := filepath.WalkDir(artifactDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Name() == process.PIDFileName {
			t.Errorf("process record was not removed: %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTerminateRegisteredHostsRejectsInvalidPID(t *testing.T) {
	artifactDir := t.TempDir()
	pidPath := filepath.Join(artifactDir, process.PIDFileName)
	if err := os.WriteFile(pidPath, []byte("not-a-pid"), 0o600); err != nil {
		t.Fatal(err)
	}

	called := false
	err := terminateRegisteredHosts(artifactDir, func(int) error {
		called = true
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "invalid pid") {
		t.Fatalf("terminateRegisteredHosts() error = %v, want invalid pid", err)
	}
	if called {
		t.Fatal("terminate function called for invalid pid")
	}
}

func assertFileContains(t *testing.T, path, pattern string) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !strings.Contains(string(content), pattern) {
		t.Fatalf("%s does not contain %q:\n%s", path, pattern, content)
	}
}

func successfulTestCommand(commands *[]string, testOutput string) streamingCommandFunc {
	return func(_ context.Context, output io.Writer, name string, args ...string) error {
		*commands = append(*commands, strings.Join(append([]string{name}, args...), " "))
		_, err := io.WriteString(output, testOutput)
		return err
	}
}

func testAzuriteEmulators() []Emulator {
	return []Emulator{
		{Name: "azurite", ArtifactFile: "azurite.log", waitReady: func(context.Context) error { return nil }},
	}
}

type emulatorFixture struct {
	containerID string
	running     bool
	log         string
	startOutput string
	startErr    error
}

func emulatorLifecycleCommand(commands *[]string, emulators map[string]emulatorFixture) commandFunc {
	return func(_ context.Context, name string, args ...string) ([]byte, error) {
		call := strings.Join(append([]string{name}, args...), " ")
		*commands = append(*commands, call)

		switch call {
		case "func --version":
			return []byte("4.12.0\n"), nil
		case "docker compose version":
			return []byte("Docker Compose version v2.39.4"), nil
		}

		if name != "docker" || len(args) < 5 {
			return nil, errors.New("unexpected command: " + call)
		}
		action := args[3]
		service := args[len(args)-1]
		fixture, known := emulators[service]
		if !known {
			return nil, errors.New("unexpected command: " + call)
		}

		switch action {
		case "ps":
			if fixture.containerID == "" {
				return nil, nil
			}
			if slices.Contains(args, "--status") && !fixture.running {
				return nil, nil
			}
			return []byte(fixture.containerID + "\n"), nil
		case "up":
			output := fixture.startOutput
			if output == "" {
				output = "started"
			}
			return []byte(output), fixture.startErr
		case "logs":
			return []byte(fixture.log), nil
		case "rm":
			return []byte("removed"), nil
		default:
			return nil, errors.New("unexpected command: " + call)
		}
	}
}

func assertCommandExecuted(t *testing.T, commands []string, want string) {
	t.Helper()
	if !slices.Contains(commands, want) {
		t.Fatalf("command %q was not executed:\n%s", want, strings.Join(commands, "\n"))
	}
}
