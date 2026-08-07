package testrunner

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type commandFunc func(context.Context, string, ...string) ([]byte, error)

// Runner owns the shared integration-test lifecycle around the Go tests:
// prerequisite validation, emulator startup/readiness, diagnostics, and cleanup.
type Runner struct {
	ComposeFile             string
	ArtifactDir             string
	TestPattern             string
	FuncExe                 string
	MinimumCoreToolsVersion string

	command   commandFunc
	waitReady func(context.Context) error
}

func (r Runner) Run(ctx context.Context) (runErr error) {
	// Unit tests inject fake command and readiness functions so they can verify
	// orchestration behavior without starting external processes.
	command := r.command
	if command == nil {
		command = runCommand
	}
	waitReady := r.waitReady
	if waitReady == nil {
		waitReady = waitForAzurite
	}
	if err := os.MkdirAll(r.ArtifactDir, 0o755); err != nil {
		return fmt.Errorf("create artifact directory: %w", err)
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

	// Query whether a container exists at all (any state) to determine ownership.
	output, err = command(ctx, "docker", append(composeArgs, "ps", "-a", "-q", "azurite")...)
	if err != nil {
		return fmt.Errorf("inspect Azurite container: %w\n%s", err, output)
	}
	owned := strings.TrimSpace(string(output)) == ""

	// Query whether the container is currently running to decide if startup is needed.
	output, err = command(ctx, "docker", append(composeArgs, "ps", "--status", "running", "-q", "azurite")...)
	if err != nil {
		return fmt.Errorf("inspect Azurite running state: %w\n%s", err, output)
	}
	running := strings.TrimSpace(string(output)) != ""

	// Register diagnostics and cleanup before startup. Docker Compose can return
	// an error after creating a container, so deferring later could leak it.
	// Named return runErr preserves the original test/startup failure; cleanup
	// failures become the result only when no earlier failure exists.
	defer func() {
		logOutput, logErr := command(context.Background(), "docker",
			append(composeArgs, "logs", "--no-color", "azurite")...)
		if writeErr := os.WriteFile(filepath.Join(r.ArtifactDir, "azurite.log"), logOutput, 0o600); runErr == nil && writeErr != nil {
			runErr = fmt.Errorf("write Azurite log: %w", writeErr)
		}
		if runErr == nil && logErr != nil {
			runErr = fmt.Errorf("collect Azurite log: %w", logErr)
		}
		if owned {
			cleanupOutput, cleanupErr := command(context.Background(), "docker",
				append(composeArgs, "rm", "-f", "-s", "azurite")...)
			if runErr == nil && cleanupErr != nil {
				runErr = fmt.Errorf("remove Azurite: %w\n%s", cleanupErr, cleanupOutput)
			}
		}
	}()

	// Reuse developer-owned containers. Only containers absent at runner startup
	// are considered runner-owned and eligible for removal.
	if !running {
		output, err = command(ctx, "docker", append(composeArgs, "up", "-d", "azurite")...)
		if err != nil {
			return fmt.Errorf("start Azurite: %w\n%s", err, output)
		}
	}

	// Do not start Core Tools until the emulator responds at the service layer.
	// A running container or open TCP port alone does not prove readiness.
	if err := waitReady(ctx); err != nil {
		return fmt.Errorf("wait for Azurite readiness: %w", err)
	}

	// Keep Go test execution as a child process so local and CI runs use the same
	// test selection, timeout, output, and exit status.
	testArgs := []string{"test", "-v", "-count=1", "-timeout", "120s", "-run", r.TestPattern, "."}
	testOutput, err := command(ctx, "go", testArgs...)
	if writeErr := os.WriteFile(filepath.Join(r.ArtifactDir, "go-test.log"), testOutput, 0o600); writeErr != nil {
		return fmt.Errorf("write Go test log: %w", writeErr)
	}
	if err != nil {
		return fmt.Errorf("run integration tests: %w\n%s", err, testOutput)
	}
	return nil
}

func runCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	return cmd.CombinedOutput()
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
