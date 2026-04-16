package integration

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

var httpEnv = map[string]string{
	"AzureWebJobsStorage":      "UseDevelopmentStorage=true",
	"FUNCTIONS_WORKER_RUNTIME": "golang",
}

func TestHttpTriggerGet(t *testing.T) {
	requireAzurite(t)
	proc := StartFuncHost(t, "httpTrigger", 7201, httpEnv, 30*time.Second)

	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/api/hello", proc.Port))
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
		fmt.Sprintf("http://localhost:%d/api/hello", proc.Port),
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
