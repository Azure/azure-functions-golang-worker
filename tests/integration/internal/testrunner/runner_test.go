package testrunner

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

// TestRunNoExistingContainer covers: two ps queries, starts Azurite, runs tests,
// captures logs, and removes the container it created.
func TestRunNoExistingContainer(t *testing.T) {
	artifactDir := t.TempDir()
	var commands []string
	var streamedOutput bytes.Buffer
	command := func(_ context.Context, name string, args ...string) ([]byte, error) {
		call := strings.Join(append([]string{name}, args...), " ")
		commands = append(commands, call)
		switch {
		case call == "func --version":
			return []byte("4.12.0\n"), nil
		case call == "docker compose version":
			return []byte("Docker Compose version v2.39.4"), nil
		case strings.Contains(call, "ps -a -q azurite"):
			return []byte(""), nil // no container
		case strings.Contains(call, "ps --status running -q azurite"):
			return []byte(""), nil // not running
		case strings.Contains(call, "up -d azurite"):
			return []byte("started azurite"), nil
		case strings.Contains(call, "logs --no-color azurite"):
			return []byte("azurite diagnostics"), nil
		case strings.Contains(call, "rm -f -s azurite"):
			return []byte("removed azurite"), nil
		default:
			return nil, errors.New("unexpected command: " + call)
		}
	}
	testCommand := successfulTestCommand(&commands, "PASS\n")

	runner := Runner{
		ComposeFile:             filepath.Join("tests", "emulators", "docker-compose.yml"),
		ArtifactDir:             artifactDir,
		TestPattern:             "^TestHttpTriggerGet$",
		FuncExe:                 "func",
		MinimumCoreToolsVersion: "4.12.0",
		command:                 command,
		testCommand:             testCommand,
		output:                  &streamedOutput,
		waitReady:               func(context.Context) error { return nil },
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

// TestRunExistingRunningContainer covers: two ps queries, no startup, no removal.
func TestRunExistingRunningContainer(t *testing.T) {
	var commands []string
	command := func(_ context.Context, name string, args ...string) ([]byte, error) {
		call := strings.Join(append([]string{name}, args...), " ")
		commands = append(commands, call)
		switch {
		case call == "func --version":
			return []byte("4.12.0\n"), nil
		case call == "docker compose version":
			return []byte("Docker Compose version v2.39.4"), nil
		case strings.Contains(call, "ps -a -q azurite"):
			return []byte("abc123\n"), nil // container exists
		case strings.Contains(call, "ps --status running -q azurite"):
			return []byte("abc123\n"), nil // container is running
		case strings.Contains(call, "logs --no-color azurite"):
			return nil, nil
		default:
			return nil, errors.New("unexpected command: " + call)
		}
	}

	runner := Runner{
		ComposeFile:             "compose.yml",
		ArtifactDir:             t.TempDir(),
		TestPattern:             "^TestHttpTriggerGet$",
		FuncExe:                 "func",
		MinimumCoreToolsVersion: "4.12.0",
		command:                 command,
		testCommand:             successfulTestCommand(&commands, "PASS\n"),
		output:                  io.Discard,
		waitReady:               func(context.Context) error { return nil },
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
	command := func(_ context.Context, name string, args ...string) ([]byte, error) {
		call := strings.Join(append([]string{name}, args...), " ")
		commands = append(commands, call)
		switch {
		case call == "func --version":
			return []byte("4.12.0\n"), nil
		case call == "docker compose version":
			return []byte("Docker Compose version v2.39.4"), nil
		case strings.Contains(call, "ps -a -q azurite"):
			return []byte("abc123\n"), nil // container exists (stopped)
		case strings.Contains(call, "ps --status running -q azurite"):
			return []byte(""), nil // not running
		case strings.Contains(call, "up -d azurite"):
			return []byte("started stopped container"), nil
		case strings.Contains(call, "logs --no-color azurite"):
			return nil, nil
		default:
			return nil, errors.New("unexpected command: " + call)
		}
	}

	runner := Runner{
		ComposeFile:             "compose.yml",
		ArtifactDir:             t.TempDir(),
		TestPattern:             "^TestHttpTriggerGet$",
		FuncExe:                 "func",
		MinimumCoreToolsVersion: "4.12.0",
		command:                 command,
		testCommand:             successfulTestCommand(&commands, "PASS\n"),
		output:                  io.Discard,
		waitReady:               func(context.Context) error { return nil },
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
	command := func(_ context.Context, name string, args ...string) ([]byte, error) {
		call := strings.Join(append([]string{name}, args...), " ")
		commands = append(commands, call)
		switch {
		case call == "func --version":
			return []byte("4.12.0\n"), nil
		case call == "docker compose version":
			return []byte("Docker Compose version v2.39.4"), nil
		case strings.Contains(call, "ps -a -q azurite"):
			return []byte(""), nil // no container before runner
		case strings.Contains(call, "ps --status running -q azurite"):
			return []byte(""), nil // not running
		case strings.Contains(call, "up -d azurite"):
			return []byte("container created before startup failure"), errors.New("compose startup failed")
		case strings.Contains(call, "logs --no-color azurite"):
			return []byte("partial startup diagnostics"), nil
		case strings.Contains(call, "rm -f -s azurite"):
			return []byte("removed partial container"), nil
		default:
			return nil, errors.New("unexpected command: " + call)
		}
	}

	err := (Runner{
		ComposeFile:             "compose.yml",
		ArtifactDir:             artifactDir,
		TestPattern:             "^TestHttpTriggerGet$",
		FuncExe:                 "func",
		MinimumCoreToolsVersion: "4.12.0",
		command:                 command,
		waitReady:               func(context.Context) error { return nil },
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
