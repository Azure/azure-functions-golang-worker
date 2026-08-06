package testrunner

import (
	"context"
	"errors"
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
		case strings.HasPrefix(call, "go test"):
			return []byte("PASS\n"), nil
		default:
			return nil, errors.New("unexpected command: " + call)
		}
	}

	runner := Runner{
		ComposeFile:      filepath.Join("tests", "emulators", "docker-compose.yml"),
		ArtifactDir:      artifactDir,
		TestPattern:      "^TestHttpTriggerGet$",
		FuncExe:          "func",
		CoreToolsVersion: "4.12.0",
		command:          command,
		waitReady:        func(context.Context) error { return nil },
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
		"go test -v -count=1 -timeout 120s -run ^TestHttpTriggerGet$ .",
		"docker compose -f " + cf + " logs --no-color azurite",
		"docker compose -f " + cf + " rm -f -s azurite",
	}
	if strings.Join(commands, "\n") != strings.Join(wantCommands, "\n") {
		t.Fatalf("commands:\n%s\nwant:\n%s", strings.Join(commands, "\n"), strings.Join(wantCommands, "\n"))
	}

	assertFileContains(t, filepath.Join(artifactDir, "go-test.log"), "PASS")
	assertFileContains(t, filepath.Join(artifactDir, "azurite.log"), "azurite diagnostics")
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
		case strings.HasPrefix(call, "go test"):
			return []byte("PASS\n"), nil
		default:
			return nil, errors.New("unexpected command: " + call)
		}
	}

	runner := Runner{
		ComposeFile:      "compose.yml",
		ArtifactDir:      t.TempDir(),
		TestPattern:      "^TestHttpTriggerGet$",
		FuncExe:          "func",
		CoreToolsVersion: "4.12.0",
		command:          command,
		waitReady:        func(context.Context) error { return nil },
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
		case strings.HasPrefix(call, "go test"):
			return []byte("PASS\n"), nil
		default:
			return nil, errors.New("unexpected command: " + call)
		}
	}

	runner := Runner{
		ComposeFile:      "compose.yml",
		ArtifactDir:      t.TempDir(),
		TestPattern:      "^TestHttpTriggerGet$",
		FuncExe:          "func",
		CoreToolsVersion: "4.12.0",
		command:          command,
		waitReady:        func(context.Context) error { return nil },
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

func TestRunRejectsUnexpectedCoreToolsVersion(t *testing.T) {
	command := func(_ context.Context, name string, args ...string) ([]byte, error) {
		call := strings.Join(append([]string{name}, args...), " ")
		if call == "func --version" {
			return []byte("4.11.0\n"), nil
		}
		return nil, errors.New("unexpected command: " + call)
	}

	err := (Runner{
		ArtifactDir:      t.TempDir(),
		FuncExe:          "func",
		CoreToolsVersion: "4.12.0",
		command:          command,
	}).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "4.12.0") || !strings.Contains(err.Error(), "4.11.0") {
		t.Fatalf("Run() error = %v, want Core Tools version mismatch", err)
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
		ComposeFile:      "compose.yml",
		ArtifactDir:      artifactDir,
		TestPattern:      "^TestHttpTriggerGet$",
		FuncExe:          "func",
		CoreToolsVersion: "4.12.0",
		command:          command,
		waitReady:        func(context.Context) error { return nil },
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
