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

// TestFlexConsumptionHttpStreamingConcurrent verifies that two long-running
// streaming requests can execute concurrently against a single worker.
//
// Background: handleBidiStream is a single-goroutine receive loop. If
// InvocationRequest is processed inline, the loop blocks for the full
// duration of the user handler — which for streaming workloads (SSE, LLM
// token streams, long-poll) is seconds to minutes. While blocked:
//   - The next InvocationRequest sits unread in the gRPC receive buffer
//     and its already-arrived HTTP request parks indefinitely on the
//     loopback listener.
//   - WorkerStatusRequest health pings queue behind it; the host's status
//     timeout fires and recycles the worker mid-stream.
//   - WorkerTerminate / FunctionEnvironmentReloadRequest queue too,
//     breaking graceful drain on deployments.
//
// Effective per-worker HTTP concurrency drops to 1 for streaming, defeating
// the feature's primary use case. The fix is to dispatch InvocationRequest
// to its own goroutine (with a Send mutex) so the receive loop keeps
// draining the stream regardless of handler duration. Python and
// .NET-isolated workers do this for the same reason.
//
// This test fires two streaming requests in parallel against one worker
// and asserts the second request's first chunk arrives before the first
// request's last chunk — a direct overlap proof. Buffered/serial dispatch
// would force the second request to wait until the first completes.
func TestFlexConsumptionHttpStreamingConcurrent(t *testing.T) {
	requireDocker(t)

	zipPath := buildSampleZipMinimal(t, "httpStreaming")

	fc := buildAndStartFlex(t, "Dockerfile.flex-test", "goworker-flex-test:latest")
	fc.waitForPing(60 * time.Second)

	fc.deployApp(zipPath)

	fc.specialize(map[string]string{
		"AzureWebJobsStorage": "UseDevelopmentStorage=true",
	})

	// Warm-up — same reason as the streaming test (avoid first-call
	// latency skewing the timing).
	warmStatus, _ := fc.sendRequestWithTimeout("GET", "/api/stream", 90*time.Second)
	if warmStatus != 200 {
		t.Fatalf("warm-up request failed: status=%d\nlogs:\n%s", warmStatus, fc.logs())
	}

	// Issue two requests concurrently. Stagger by a small amount so we
	// don't accidentally have both arrive at the host on exactly the
	// same nanosecond — the 250 ms head start gives the host time to
	// dispatch the first one before the second arrives, which is the
	// realistic production pattern.
	type result struct {
		label   string
		events  []arrival
		err     error
		started time.Time
	}

	runOne := func(label string, delay time.Duration) result {
		time.Sleep(delay)
		started := time.Now()

		client := &http.Client{Timeout: 30 * time.Second}
		req, err := http.NewRequest("GET", fc.url()+"/api/stream", nil)
		if err != nil {
			return result{label: label, err: fmt.Errorf("NewRequest: %w", err)}
		}
		fc.addAuthHeaders(req)

		resp, err := client.Do(req)
		if err != nil {
			return result{label: label, err: fmt.Errorf("client Do: %w", err)}
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			return result{label: label, err: fmt.Errorf("status=%d", resp.StatusCode)}
		}

		var events []arrival
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if !strings.HasPrefix(line, "data: event ") {
				continue
			}
			events = append(events, arrival{line: line, at: time.Now()})
		}
		if err := scanner.Err(); err != nil {
			return result{label: label, err: fmt.Errorf("scan: %w", err)}
		}

		return result{label: label, events: events, started: started}
	}

	results := make(chan result, 2)
	go func() { results <- runOne("A", 0) }()
	go func() { results <- runOne("B", 250*time.Millisecond) }()

	r1 := <-results
	r2 := <-results
	// Sort so r1 is whichever started first — makes the assertion phrasing
	// independent of which goroutine completed first.
	if r2.started.Before(r1.started) {
		r1, r2 = r2, r1
	}

	for _, r := range []result{r1, r2} {
		if r.err != nil {
			t.Fatalf("request %s failed: %v\nlogs:\n%s", r.label, r.err, fc.logs())
		}
		if got, want := len(r.events), 5; got != want {
			t.Fatalf("request %s: expected %d events, got %d\nlogs:\n%s", r.label, want, got, fc.logs())
		}
	}

	// Diagnostic dump.
	earliest := r1.started
	t.Logf("request %s started at +0s, %d events:", r1.label, len(r1.events))
	for i, a := range r1.events {
		t.Logf("  [%s][%02d] +%-8v %q", r1.label, i, a.at.Sub(earliest).Round(10*time.Millisecond), a.line)
	}
	t.Logf("request %s started at +%v, %d events:", r2.label, r2.started.Sub(earliest).Round(10*time.Millisecond), len(r2.events))
	for i, a := range r2.events {
		t.Logf("  [%s][%02d] +%-8v %q", r2.label, i, a.at.Sub(earliest).Round(10*time.Millisecond), a.line)
	}

	// Concurrency proof: the second request's *first* event must arrive
	// before the first request's *last* event. If the dispatcher were
	// serializing invocations, request B's event 0 would not arrive until
	// after request A finished its full ~5-second sequence — i.e., after
	// A's event 4.
	r1Last := r1.events[len(r1.events)-1].at
	r2First := r2.events[0].at

	if !r2First.Before(r1Last) {
		t.Fatalf("requests appear to be serialized — %s's first event arrived at +%v, %s's last event arrived at +%v\n"+
			"this indicates the gRPC dispatcher is processing InvocationRequest synchronously, blocking concurrent streaming\n"+
			"logs:\n%s",
			r2.label, r2First.Sub(earliest), r1.label, r1Last.Sub(earliest), fc.logs())
	}

	t.Logf("PASS: %s.event[0] arrived at +%v, before %s.event[last] at +%v — requests overlapped",
		r2.label, r2First.Sub(earliest).Round(10*time.Millisecond),
		r1.label, r1Last.Sub(earliest).Round(10*time.Millisecond))
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
