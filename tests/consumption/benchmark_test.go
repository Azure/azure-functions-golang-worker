package consumption

import (
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

// BenchmarkHttpTriggerDirect benchmarks HTTP trigger latency with the
// skipPlaceholderInit host fix (direct host-to-worker, no proxy).
// Requires CUSTOM_HOST_PATH env var pointing to the host publish output.
func BenchmarkHttpTriggerDirect(b *testing.B) {
	fc := setupBenchContainer(b, "direct")
	benchmarkRequests(b, fc)
}

// BenchmarkHttpTriggerProxy benchmarks HTTP trigger latency with the proxy
// sitting between host and worker (extra gRPC hop on localhost).
func BenchmarkHttpTriggerProxy(b *testing.B) {
	fc := setupBenchContainer(b, "proxy")
	benchmarkRequests(b, fc)
}

func benchmarkRequests(b *testing.B, fc *flexContainer) {
	client := &http.Client{Timeout: 5 * time.Second}
	url := fc.url() + "/api/hello"

	// Warm up
	for i := 0; i < 50; i++ {
		req, _ := http.NewRequest("GET", url, nil)
		fc.addAuthHeaders(req)
		resp, err := client.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req, _ := http.NewRequest("GET", url, nil)
		fc.addAuthHeaders(req)
		resp, err := client.Do(req)
		if err != nil {
			b.Fatalf("request failed: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			b.Fatalf("expected 200, got %d", resp.StatusCode)
		}
	}
}

func setupBenchContainer(b *testing.B, mode string) *flexContainer {
	b.Helper()
	requireDocker(b)

	var fc *flexContainer
	switch mode {
	case "direct":
		fc = startFlexContainerHostFix(b)
	case "proxy":
		fc = buildAndStartFlex(b, "Dockerfile.flex-test-proxy-bench", "goworker-flex-bench-proxy:latest")
	default:
		b.Fatalf("unknown mode: %s", mode)
	}

	fc.waitForPing(60 * time.Second)

	zipPath := buildSampleZipMinimal(b, "httpTrigger")
	fc.deployApp(zipPath)

	fc.specialize(map[string]string{
		"AzureWebJobsStorage": "UseDevelopmentStorage=true",
	})

	// Wait for the function to be loaded
	client := &http.Client{Timeout: 10 * time.Second}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest("GET", fc.url()+"/api/hello", nil)
		fc.addAuthHeaders(req)
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			fmt.Printf("[%s] Container ready at %s\n", mode, fc.url())
			return fc
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(2 * time.Second)
	}
	b.Fatalf("[%s] function never became ready\nlogs:\n%s", mode, fc.logs())
	return nil
}
