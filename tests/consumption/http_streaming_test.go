package consumption

import (
	"bufio"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestFlexConsumptionHttpStreaming verifies end-to-end HTTP streaming
// through the full proxy worker stack:
//
//   container ┌──────────────────────────────────────────────────┐
//             │ host ── YARP ──► proxy ── exec ──► user app      │
//             │                                       │           │
//             │                          embedded http.Server     │
//             │                                       │           │
//             │                              user handler runs    │
//             │                              with live            │
//             │                              ResponseWriter +     │
//             │                              http.Flusher         │
//             └───────────┬──────────────────────────────────────┘
//                         │ chunked transfer encoding
//             ┌───────────▼──────────┐
//             │  test client reads   │
//             │  chunks over time    │
//             └──────────────────────┘
//
// The httpStreaming sample emits 5 SSE events at 1-second intervals.
// If streaming is *broken* (buffered), the test client receives all 5
// events at once at ~t=5s. If streaming *works*, the events arrive
// incrementally at ~t=0, 1, 2, 3, 4s.
//
// We assert two things:
//  1. At least 5 events appear in the body.
//  2. The wall-clock time between the first chunk and the last chunk is
//     greater than a conservative threshold — proving the events were
//     not buffered into a single response.
func TestFlexConsumptionHttpStreaming(t *testing.T) {
	requireDocker(t)

	zipPath := buildSampleZipMinimal(t, "httpStreaming")

	fc := buildAndStartFlex(t, "Dockerfile.flex-test", "goworker-flex-test:latest")
	fc.waitForPing(60 * time.Second)

	fc.deployApp(zipPath)

	fc.specialize(map[string]string{
		"AzureWebJobsStorage": "UseDevelopmentStorage=true",
	})

	// Wait for at least one successful request before timing the streaming
	// run. The first call after specialize includes worker startup latency
	// (binary exec, gRPC handshake, HttpUri registration, YARP wire-up)
	// which would skew our timing measurements. Hitting it once warms
	// everything up.
	warmStatus, _ := fc.sendRequestWithTimeout("GET", "/api/stream", 90*time.Second)
	if warmStatus != 200 {
		t.Fatalf("warm-up request failed: status=%d\nlogs:\n%s", warmStatus, fc.logs())
	}

	// Now the timed run. Use a long client-side timeout because the
	// handler intentionally sleeps for ~5 seconds total.
	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest("GET", fc.url()+"/api/stream", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	fc.addAuthHeaders(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("client Do: %v\nlogs:\n%s", err, fc.logs())
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 from /api/stream, got %d\nlogs:\n%s", resp.StatusCode, fc.logs())
	}

	// Read the body line-by-line and timestamp each non-empty SSE data
	// line as it arrives. bufio.Scanner returns control to us as soon as
	// a newline is seen, which (combined with chunked transfer encoding
	// from the upstream Flush()) gives us per-chunk arrival times.
	var arrivals []arrival

	start := time.Now()
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		arrivals = append(arrivals, arrival{line: line, at: time.Now()})
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanning body: %v", err)
	}

	// Diagnostic logging — useful when investigating regressions.
	t.Logf("received %d non-empty lines over %v", len(arrivals), time.Since(start))
	for i, a := range arrivals {
		t.Logf("  [%02d] +%-8v %q", i, a.at.Sub(start).Round(10*time.Millisecond), a.line)
	}

	// Filter to the 5 SSE event lines emitted by the sample.
	var events []arrival
	for _, a := range arrivals {
		if strings.HasPrefix(a.line, "data: event ") {
			events = append(events, a)
		}
	}

	if got, want := len(events), 5; got != want {
		t.Fatalf("expected %d 'data: event N' lines, got %d\nfull arrivals:\n%s\nlogs:\n%s",
			want, got, formatArrivals(arrivals, start), fc.logs())
	}

	// The handler sleeps ~1s between events, so the sample naturally takes
	// ~4-5s end-to-end. Buffered responses arrive in a single chunk —
	// first.at would equal last.at within milliseconds. Use a conservative
	// 2-second floor that's well below the ~4-second expected spread but
	// far above any realistic buffered-arrival jitter.
	spread := events[len(events)-1].at.Sub(events[0].at)
	const minSpread = 2 * time.Second
	if spread < minSpread {
		t.Fatalf("events appear to have been buffered: spread between first and last event = %v (want > %v)\n"+
			"this indicates the worker is NOT streaming end-to-end (HttpUri capability not honored or http.Flusher buffered)\n"+
			"events:\n%s\nlogs:\n%s",
			spread, minSpread, formatArrivals(events, start), fc.logs())
	}

	t.Logf("PASS: streaming spread = %v (>%v)", spread, minSpread)
}

// TestFlexConsumptionHttpStreamingOptOut verifies the FUNCTIONS_GO_DISABLE_HTTP_PROXY
// app-setting forces the worker back onto the legacy gRPC body path, even
// when the host supports the HttpUri capability.
//
// We deploy the plain httpTrigger sample (not httpStreaming) because the
// opt-out path uses the legacy ResponseWriterProxy which does NOT implement
// http.Flusher — the streaming sample's defensive `if !ok { http.Error(...) }`
// guard would early-return 500 on that path, which is correct behavior for
// a streaming demo but irrelevant to what we're verifying here.
//
// The two assertions are:
//
//  1. The opt-out log line appears in container logs — proving the env var
//     propagated all the way from the encrypted assign context, through the
//     host, into the worker child process before startHTTPProxy ran.
//
//  2. /api/hello returns 200 with the expected body — proving the gRPC body
//     path still works end-to-end after opting out.
//
// Together these prove the opt-out switch is operational and doesn't break
// non-streaming HTTP triggers.
func TestFlexConsumptionHttpStreamingOptOut(t *testing.T) {
	requireDocker(t)

	zipPath := buildSampleZipMinimal(t, "httpTrigger")

	fc := buildAndStartFlex(t, "Dockerfile.flex-test", "goworker-flex-test:latest")
	fc.waitForPing(60 * time.Second)

	fc.deployApp(zipPath)

	// The opt-out: ride the encrypted-context env map straight into the
	// worker process. The host sets these as environment variables before
	// exec'ing the worker, so by the time startHTTPProxy runs the env var
	// is already in os.Environ().
	fc.specialize(map[string]string{
		"AzureWebJobsStorage":             "UseDevelopmentStorage=true",
		"FUNCTIONS_GO_DISABLE_HTTP_PROXY": "1",
	})

	status, body := fc.sendRequestWithTimeout("GET", "/api/hello", 90*time.Second)
	if status != 200 {
		t.Fatalf("expected 200 from /api/hello on opt-out path, got %d\nlogs:\n%s", status, fc.logs())
	}
	if string(body) != "Hello from Go Worker!" {
		t.Fatalf("expected 'Hello from Go Worker!' on opt-out path, got %q\nlogs:\n%s", string(body), fc.logs())
	}

	// Confirm the worker actually entered the opt-out path. This is the
	// load-bearing assertion — without it, the test would still pass on the
	// HTTP-proxy path since httpTrigger works on both.
	logs := fc.logs()
	const optOutLog = "HTTP proxy: disabled via FUNCTIONS_GO_DISABLE_HTTP_PROXY"
	if !strings.Contains(logs, optOutLog) {
		t.Fatalf("expected opt-out log line %q not found — env var may not be propagating to the worker process\nlogs:\n%s",
			optOutLog, logs)
	}

	t.Logf("PASS: opt-out engaged (log line found), gRPC body path served /api/hello correctly")
}

func formatArrivals(arrivals []arrival, start time.Time) string {
	var b strings.Builder
	for i, a := range arrivals {
		fmt.Fprintf(&b, "  [%02d] +%-8v %q\n", i, a.at.Sub(start).Round(10*time.Millisecond), a.line)
	}
	return b.String()
}

// arrival is declared in this file so the test stays self-contained.
type arrival struct {
	line string
	at   time.Time
}
