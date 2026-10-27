package integration

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/azure/azure-functions-golang-worker/tests/integration/internal/testhost"
)

const durableStorageEnv = "DURABLE_E2E_STORAGE_CONNECTION_STRING"

const durableStorage = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
	"AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
	"BlobEndpoint=http://127.0.0.1:17310/devstoreaccount1;" +
	"QueueEndpoint=http://127.0.0.1:17311/devstoreaccount1;" +
	"TableEndpoint=http://127.0.0.1:17312/devstoreaccount1;"

// Unlike startFunctionHost, this path never deletes shared trigger checkpoints
// or receipts. Its default storage is the dedicated durable-azurite service.
func startDurableHost(t *testing.T, overrides map[string]string) (testhost.Host, func(*testing.T) testhost.Host) {
	t.Helper()
	storage := os.Getenv(durableStorageEnv)
	if storage == "" {
		storage = durableStorage
	}
	if strings.EqualFold(strings.TrimSpace(storage), "UseDevelopmentStorage=true") {
		t.Fatalf("%s must use test-owned storage, not shared development storage (see README)", durableStorageEnv)
	}
	appDir := filepath.Join(integrationTestDataDir(), "durableFunctions")
	executable := filepath.Join(appDir, "bin", "app")
	if runtime.GOOS == "windows" {
		executable += ".exe"
	}
	if err := os.MkdirAll(filepath.Dir(executable), 0o755); err != nil {
		t.Fatal(err)
	}
	buildCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", executable, ".")
	build.Dir = appDir
	build.Env = append(os.Environ(), "GOWORK=off")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build durable fixture: %v\n%s", err, output)
	}

	id := strings.ToLower(rand.Text())
	env := map[string]string{
		"GOWORK":                        "off",
		"AzureWebJobsStorage":           storage,
		"AzureFunctionsWebHost__hostid": id,
		"AzureFunctionsJobHost__extensions__durableTask__hubName": "GoTest" + id,
		"languageWorkers__native__defaultExecutablePath":          executable,
	}
	for key, value := range overrides {
		env[key] = value
	}
	var current testhost.Host
	generation := 0
	start := func(t *testing.T) testhost.Host {
		t.Helper()
		generation++
		host, err := testhost.Start(context.Background(), testhost.Config{
			SampleDir: appDir, FuncExe: funcExe(), NoBuild: true, Environment: env,
			ArtifactDir: filepath.Join("artifacts", t.Name(), fmt.Sprint(generation)),
			InitTimeout: durableStartTimeout,
		})
		if err != nil {
			t.Fatalf("start isolated durable host: %v", err)
		}
		current = host
		t.Logf("Durable task hub %s, host log %s",
			env["AzureFunctionsJobHost__extensions__durableTask__hubName"], host.LogPath())
		// Worker readiness precedes HTTP route readiness on some Core Tools builds.
		deadline := time.Now().Add(20 * time.Second)
		client := &http.Client{Timeout: 2 * time.Second}
		for {
			resp, err := client.Get(host.URL() + "/api/expenses/readiness-" + id)
			if err == nil {
				body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
				resp.Body.Close()
				if readErr == nil && resp.StatusCode == http.StatusNotFound && strings.Contains(string(body), "no such instance") {
					return host
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("durable routes not ready, see %s (last error %v)", host.LogPath(), err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	stop := func() error {
		if current == nil {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return current.Stop(ctx)
	}
	t.Cleanup(func() {
		if err := stop(); err != nil {
			t.Errorf("stop isolated durable host: %v", err)
		}
	})
	host := start(t)
	return host, func(t *testing.T) testhost.Host {
		t.Helper()
		if err := stop(); err != nil {
			t.Fatalf("stop durable host before restart: %v", err)
		}
		return start(t)
	}
}
