package integration

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azure/azure-functions-golang-worker/tests/integration/internal/testhost"
)

var httpEnv = map[string]string{
	"AzureWebJobsStorage": "UseDevelopmentStorage=true",
}

func TestHttpTriggerGet(t *testing.T) {
	requireAzurite(t)
	host, err := testhost.Start(context.Background(), testhost.Config{
		SampleDir:   filepath.Join(samplesDir(), "httpTrigger"),
		FuncExe:     funcExe(),
		Environment: httpEnv,
		ArtifactDir: filepath.Join("artifacts", t.Name()),
		InitTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("start HTTP function host: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := host.Stop(stopCtx); err != nil {
			t.Errorf("stop HTTP function host: %v", err)
		}
	})

	resp, err := http.Get(host.URL() + "/api/hello")
	if err != nil {
		t.Fatalf("HTTP GET failed: %v", err)
	}
	body := readAll(t, resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if string(body) != "Hello from Go Worker!" {
		t.Fatalf("expected 'Hello from Go Worker!', got %q", string(body))
	}

	// POST to the same host instance
	resp, err = http.Post(
		host.URL()+"/api/hello",
		"text/plain",
		strings.NewReader("test body"),
	)
	if err != nil {
		t.Fatalf("HTTP POST failed: %v", err)
	}
	body = readAll(t, resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if string(body) != "Hello from Go Worker!" {
		t.Fatalf("expected 'Hello from Go Worker!', got %q", string(body))
	}
}
