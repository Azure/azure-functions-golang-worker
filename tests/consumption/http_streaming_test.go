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
