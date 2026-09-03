package durabletask

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
)

func TestWriteCheckStatusResponse_UsesHostSuppliedBaseURL(t *testing.T) {
	// The host advertises a base that already includes the durable webhook path
	// and the query string management calls must carry.
	client := &Client{
		httpBaseURL: "http://localhost:7071/runtime/webhooks/durabletask",
		queryParams: "code=secret&taskHub=GoDurableHub",
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "http://example.test/api/hello", nil)

	if err := client.WriteCheckStatusResponse(recorder, request, "abc-123"); err != nil {
		t.Fatalf("write check status response: %v", err)
	}

	if recorder.Code != http.StatusAccepted {
		t.Errorf("expected status 202, got %d", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("expected a JSON content type, got %q", got)
	}
	if got := recorder.Header().Get("Retry-After"); got != checkStatusRetryAfterSeconds {
		t.Errorf("expected a Retry-After hint of %q, got %q", checkStatusRetryAfterSeconds, got)
	}

	var urls HTTPManagementURLs
	if err := json.Unmarshal(recorder.Body.Bytes(), &urls); err != nil {
		t.Fatalf("decode body %q: %v", recorder.Body.String(), err)
	}

	if urls.ID != "abc-123" {
		t.Errorf("expected the instance ID in the body, got %q", urls.ID)
	}

	wantStatus := "http://localhost:7071/runtime/webhooks/durabletask/instances/abc-123?code=secret&taskHub=GoDurableHub"
	if urls.StatusQueryGetURI != wantStatus {
		t.Errorf("status URI:\n  want %s\n  got  %s", wantStatus, urls.StatusQueryGetURI)
	}
	// Location must match the status URI so callers can follow it directly.
	if got := recorder.Header().Get("Location"); got != wantStatus {
		t.Errorf("Location header:\n  want %s\n  got  %s", wantStatus, got)
	}

	// The event name and reason stay as placeholders for the caller to fill in.
	if !strings.Contains(urls.SendEventPostURI, "/raiseEvent/{eventName}") {
		t.Errorf("expected an {eventName} placeholder, got %s", urls.SendEventPostURI)
	}
	for name, uri := range map[string]string{
		"terminate": urls.TerminatePostURI,
		"suspend":   urls.SuspendPostURI,
		"resume":    urls.ResumePostURI,
	} {
		if !strings.Contains(uri, "reason={text}") {
			t.Errorf("expected a reason placeholder on the %s URI, got %s", name, uri)
		}
	}
}

// Clients created from EnvGrpcEndpoint carry no host-supplied base, so the URLs
// come from the request instead.
func TestWriteCheckStatusResponse_FallsBackToRequest(t *testing.T) {
	client := &Client{}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "http://myapp.example/api/hello", nil)

	if err := client.WriteCheckStatusResponse(recorder, request, "xyz-789"); err != nil {
		t.Fatalf("write check status response: %v", err)
	}

	var urls HTTPManagementURLs
	if err := json.Unmarshal(recorder.Body.Bytes(), &urls); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	want := "http://myapp.example/runtime/webhooks/durabletask/instances/xyz-789"
	if urls.StatusQueryGetURI != want {
		t.Errorf("status URI:\n  want %s\n  got  %s", want, urls.StatusQueryGetURI)
	}
}

// Behind the Functions front end the inbound connection is plain HTTP, so the
// forwarding headers decide the externally visible origin.
func TestWriteCheckStatusResponse_HonorsForwardedHeaders(t *testing.T) {
	client := &Client{}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "http://internal:8080/api/hello", nil)
	request.Header.Set("X-Forwarded-Proto", "https")
	request.Header.Set("X-Forwarded-Host", "myapp.azurewebsites.net")

	if err := client.WriteCheckStatusResponse(recorder, request, "id-1"); err != nil {
		t.Fatalf("write check status response: %v", err)
	}

	var urls HTTPManagementURLs
	if err := json.Unmarshal(recorder.Body.Bytes(), &urls); err != nil {
		t.Fatalf("decode body: %v", err)
	}

	want := "https://myapp.azurewebsites.net/runtime/webhooks/durabletask/instances/id-1"
	if urls.StatusQueryGetURI != want {
		t.Errorf("status URI:\n  want %s\n  got  %s", want, urls.StatusQueryGetURI)
	}
}

// A custom domain or an API gateway is only visible through the forwarding
// headers: the binding's base URL is built by the host without reference to
// any request, so it always names the app's default hostname. The forwarded
// origin therefore wins, while the path and query still come from the binding
// because only the host knows the webhook root and the required query string.
func TestManagementURLs_ForwardedOriginOverridesBindingOrigin(t *testing.T) {
	client := &Client{
		httpBaseURL: "https://myapp.azurewebsites.net/runtime/webhooks/durabletask",
		queryParams: "code=secret&taskHub=GoDurableHub",
	}

	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:36833/api/hello", nil)
	request.Header.Set("X-Forwarded-Proto", "https")
	request.Header.Set("X-Forwarded-Host", "orders.contoso.com")

	urls := client.ManagementURLs(request, "id-1")

	want := "https://orders.contoso.com/runtime/webhooks/durabletask/instances/id-1?code=secret&taskHub=GoDurableHub"
	if urls.StatusQueryGetURI != want {
		t.Errorf("status URI:\n  want %s\n  got  %s", want, urls.StatusQueryGetURI)
	}
}

// The host forwards requests to the worker over loopback, so r.Host names the
// worker's own listener. Without a forwarded host we must fall back to the
// binding rather than emitting an unreachable 127.0.0.1 URL.
func TestManagementURLs_IgnoresLoopbackRequestHostWhenBindingPresent(t *testing.T) {
	client := &Client{httpBaseURL: "https://myapp.azurewebsites.net/runtime/webhooks/durabletask"}

	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:36833/api/hello", nil)
	urls := client.ManagementURLs(request, "id-1")

	want := "https://myapp.azurewebsites.net/runtime/webhooks/durabletask/instances/id-1"
	if urls.StatusQueryGetURI != want {
		t.Errorf("status URI:\n  want %s\n  got  %s", want, urls.StatusQueryGetURI)
	}
}

// A non-default webhook root advertised by the host must survive the swap to
// the forwarded origin.
func TestManagementURLs_KeepsBindingWebhookPath(t *testing.T) {
	client := &Client{httpBaseURL: "https://myapp.azurewebsites.net/custom/webhooks/durabletask"}

	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:36833/api/hello", nil)
	request.Header.Set("X-Forwarded-Host", "orders.contoso.com")

	urls := client.ManagementURLs(request, "id-1")

	want := "https://orders.contoso.com/custom/webhooks/durabletask/instances/id-1"
	if urls.StatusQueryGetURI != want {
		t.Errorf("status URI:\n  want %s\n  got  %s", want, urls.StatusQueryGetURI)
	}
}

// The in-process extension, the .NET isolated worker and the JavaScript worker
// all emit rewindPostUri, so the payload shape matches theirs.
func TestManagementURLs_IncludesRewind(t *testing.T) {
	client := &Client{httpBaseURL: "http://localhost:7071/runtime/webhooks/durabletask"}

	request := httptest.NewRequest(http.MethodPost, "http://example.test/api/hello", nil)
	urls := client.ManagementURLs(request, "id-1")

	want := "http://localhost:7071/runtime/webhooks/durabletask/instances/id-1/rewind?reason={text}"
	if urls.RewindPostURI != want {
		t.Errorf("rewind URI:\n  want %s\n  got  %s", want, urls.RewindPostURI)
	}
}

// The status payload must match what the host's own management API returns,
// so an app's endpoint and the statusQueryGetUri from a check-status reply are
// interchangeable. In particular the serialized values are embedded as raw
// JSON: an object stays an object rather than becoming a quoted string.
func TestWriteStatusResponse_MatchesHostPayloadShape(t *testing.T) {
	created := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	updated := created.Add(2 * time.Minute)

	meta := &api.OrchestrationMetadata{
		InstanceID:      "id-1",
		Name:            "ProcessExpense",
		RuntimeStatus:   api.RUNTIME_STATUS_RUNNING,
		SerializedInput: `{"amount":750}`,
		// SetCustomStatus takes a plain string and stores it verbatim, so this
		// is not JSON and has to be quoted on the way out.
		SerializedCustomStatus: `awaiting manager approval`,
		CreatedAt:              created,
		LastUpdatedAt:          updated,
	}

	recorder := httptest.NewRecorder()
	if err := WriteStatusResponse(recorder, meta); err != nil {
		t.Fatalf("write status response: %v", err)
	}

	if recorder.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("expected a JSON content type, got %q", got)
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body %q: %v", recorder.Body.String(), err)
	}

	// The host reports the short status name, not the protobuf enum.
	if got := string(body["runtimeStatus"]); got != `"Running"` {
		t.Errorf("runtimeStatus = %s, want \"Running\"", got)
	}
	// Input is embedded, not re-encoded as a string.
	if got := string(body["input"]); got != `{"amount":750}` {
		t.Errorf("input = %s, want the object embedded as-is", got)
	}
	if got := string(body["customStatus"]); got != `"awaiting manager approval"` {
		t.Errorf("customStatus = %s", got)
	}
	// An orchestration still running has produced no output.
	if got := string(body["output"]); got != "null" {
		t.Errorf("output = %s, want null", got)
	}
	// The host's field names are createdTime / lastUpdatedTime.
	for _, field := range []string{"name", "instanceId", "createdTime", "lastUpdatedTime"} {
		if _, ok := body[field]; !ok {
			t.Errorf("missing %q in the status payload", field)
		}
	}
}

func TestRuntimeStatus(t *testing.T) {
	tests := []struct {
		status api.OrchestrationStatus
		want   string
	}{
		{api.RUNTIME_STATUS_RUNNING, "Running"},
		{api.RUNTIME_STATUS_COMPLETED, "Completed"},
		{api.RUNTIME_STATUS_CONTINUED_AS_NEW, "ContinuedAsNew"},
		{api.RUNTIME_STATUS_TERMINATED, "Terminated"},
	}
	for _, test := range tests {
		got := RuntimeStatus(&api.OrchestrationMetadata{RuntimeStatus: test.status})
		if got != test.want {
			t.Errorf("RuntimeStatus(%v) = %q, want %q", test.status, got, test.want)
		}
	}
	if got := RuntimeStatus(nil); got != "" {
		t.Errorf("RuntimeStatus(nil) = %q, want empty", got)
	}
}

func TestManagementURLs_EscapesInstanceID(t *testing.T) {
	client := &Client{httpBaseURL: "http://localhost:7071/runtime/webhooks/durabletask"}

	request := httptest.NewRequest(http.MethodPost, "http://example.test/api/hello", nil)
	urls := client.ManagementURLs(request, "id with/slash")

	if strings.Contains(urls.StatusQueryGetURI, "id with/slash") {
		t.Errorf("expected the instance ID to be escaped, got %s", urls.StatusQueryGetURI)
	}
}

func TestWithQuery(t *testing.T) {
	cases := []struct {
		name      string
		base      string
		fragments []string
		want      string
	}{
		{"no fragments", "http://x/y", nil, "http://x/y"},
		{"all empty", "http://x/y", []string{"", ""}, "http://x/y"},
		{"one fragment", "http://x/y", []string{"code=1"}, "http://x/y?code=1"},
		{"skips empties", "http://x/y", []string{"reason={text}", "", "code=1"}, "http://x/y?reason={text}&code=1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := withQuery(tc.base, tc.fragments...); got != tc.want {
				t.Errorf("want %s, got %s", tc.want, got)
			}
		})
	}
}
