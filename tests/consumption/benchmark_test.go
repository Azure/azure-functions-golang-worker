package consumption

import (
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

// BenchmarkHttpTriggerDirect benchmarks HTTP trigger latency with the proxy's
// exec bypass (direct host-to-worker, no proxy). The app binary is prebaked
// into the image so the proxy execs into it immediately on startup.
func BenchmarkHttpTriggerDirect(b *testing.B) {
	b.Helper()
	requireDocker(b)

	fc := buildAndStartFlex(b, "Dockerfile.flex-test-direct-bench", "goworker-flex-bench-direct:latest")
	fc.waitForPing(60 * time.Second)

	fc.specialize(map[string]string{
		"AzureWebJobsStorage": "UseDevelopmentStorage=true",
	})

	waitForFunction(b, fc)
	benchmarkRequests(b, fc)
}

// BenchmarkHttpTriggerProxy benchmarks HTTP trigger latency with the proxy
// sitting between host and worker (extra gRPC hop on localhost).
// This is the production path for flex consumption.
func BenchmarkHttpTriggerProxy(b *testing.B) {
	b.Helper()
	requireDocker(b)

	fc := buildAndStartFlex(b, "Dockerfile.flex-test", "goworker-flex-bench:latest")
	fc.waitForPing(60 * time.Second)

	zipPath := buildSampleZipMinimal(b, "httpTrigger")
	fc.deployApp(zipPath)

	fc.specialize(map[string]string{
		"AzureWebJobsStorage": "UseDevelopmentStorage=true",
	})

	waitForFunction(b, fc)
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

func waitForFunction(b *testing.B, fc *flexContainer) {
	b.Helper()
	client := &http.Client{Timeout: 10 * time.Second}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest("GET", fc.url()+"/api/hello", nil)
		fc.addAuthHeaders(req)
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			fmt.Printf("Container ready at %s\n", fc.url())
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(2 * time.Second)
	}
	b.Fatalf("function never became ready\nlogs:\n%s", fc.logs())
}
