package durabletask

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
