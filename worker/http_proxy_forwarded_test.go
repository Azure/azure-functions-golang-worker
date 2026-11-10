package worker

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
	"github.com/azure/azure-functions-golang-worker/worker/proto/shared"
)

func TestApplyForwardedOrigin(t *testing.T) {
	const loopback = "127.0.0.1:5000"

	tests := []struct {
		name       string
		host       string
		proto      string
		wantHost   string
		wantScheme string
		wantURLHst string
	}{
		{name: "host and proto", host: "example.com", proto: "https", wantHost: "example.com", wantScheme: "https", wantURLHst: "example.com"},
		{name: "host with port", host: "example.com:8443", proto: "https", wantHost: "example.com:8443", wantScheme: "https", wantURLHst: "example.com:8443"},
		{name: "proto is case insensitive", host: "example.com", proto: "HTTPS", wantHost: "example.com", wantScheme: "https", wantURLHst: "example.com"},
		{name: "first entry of a chain wins", host: "client.example.com, proxy.internal", proto: "https, http", wantHost: "client.example.com", wantScheme: "https", wantURLHst: "client.example.com"},
		{name: "ipv6 host", host: "[::1]:8080", proto: "http", wantHost: "[::1]:8080", wantScheme: "http", wantURLHst: "[::1]:8080"},
		{name: "host alone sets only r.Host", host: "example.com", wantHost: "example.com"},
		{name: "proto alone changes nothing", proto: "https", wantHost: loopback},
		{name: "unknown proto sets only r.Host", host: "example.com", proto: "ftp", wantHost: "example.com"},
		{name: "neither header changes nothing", wantHost: loopback},
		{name: "path in host is rejected", host: "example.com/evil", proto: "https", wantHost: loopback},
		{name: "credentials in host are rejected", host: "user@example.com", proto: "https", wantHost: loopback},
		{name: "whitespace in host is rejected", host: "exa mple.com", proto: "https", wantHost: loopback},
		{name: "query in host is rejected", host: "example.com?x=1", proto: "https", wantHost: loopback},
		{name: "port only is rejected", host: ":8080", proto: "https", wantHost: loopback},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/api/orders/7?x=1", nil)
			r.Host = loopback
			if tc.host != "" {
				r.Header.Set("X-Forwarded-Host", tc.host)
			}
			if tc.proto != "" {
				r.Header.Set("X-Forwarded-Proto", tc.proto)
			}

			applyForwardedOrigin(r)

			if r.Host != tc.wantHost {
				t.Errorf("Host = %q, want %q", r.Host, tc.wantHost)
			}
			if r.URL.Scheme != tc.wantScheme {
				t.Errorf("URL.Scheme = %q, want %q", r.URL.Scheme, tc.wantScheme)
			}
			if r.URL.Host != tc.wantURLHst {
				t.Errorf("URL.Host = %q, want %q", r.URL.Host, tc.wantURLHst)
			}
			if r.URL.Path != "/api/orders/7" || r.URL.RawQuery != "x=1" {
				t.Errorf("path and query must be untouched, got %q?%q", r.URL.Path, r.URL.RawQuery)
			}
			if got := r.Header.Get("X-Forwarded-Host"); got != tc.host {
				t.Errorf("X-Forwarded-Host = %q, want it left in place as %q", got, tc.host)
			}
			if got := r.Header.Get("X-Forwarded-Proto"); got != tc.proto {
				t.Errorf("X-Forwarded-Proto = %q, want it left in place as %q", got, tc.proto)
			}
		})
	}
}

// TestHTTPProxy_RestoresOriginAndRouteParams drives a request through the
// real loopback listener the way the host does. The handler must see the
// caller-visible origin, and the route parameters the host sent on the
// trigger input, with the request headers intact.
func TestHTTPProxy_RestoresOriginAndRouteParams(t *testing.T) {
	type seen struct {
		host, scheme, urlHost, id, tenant, absent, requestID string
	}
	got := make(chan seen, 1)

	app := sdk.FunctionApp()
	app.HTTP("orders", func(w http.ResponseWriter, r *http.Request) {
		got <- seen{
			host:      r.Host,
			scheme:    r.URL.Scheme,
			urlHost:   r.URL.Host,
			id:        r.PathValue("id"),
			tenant:    r.PathValue("tenant"),
			absent:    r.PathValue("missing"),
			requestID: r.Header.Get("X-Request-Id"),
		}
		w.WriteHeader(http.StatusOK)
	})

	proxy := startHTTPProxy(app)
	if proxy == nil {
		t.Fatal("expected HTTP proxy to start")
	}
	t.Cleanup(func() { _ = proxy.shutdown(nil) })

	var rf *sdk.RegisteredFunction
	app.GetRegisteredFunctions().Range(func(_, value any) bool {
		rf = value.(*sdk.RegisteredFunction)
		return false
	})
	if rf == nil {
		t.Fatal("registered function not found")
	}
	loaded := &LoadedFunction{Function: *rf}

	// The host sends route data, and nothing else, on the trigger input when
	// the request travels over the proxy hop.
	const invocationID = "test-invocation-origin"
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = proxy.notifyGRPCArrival(&pb.InvocationRequest{
			InvocationId: invocationID,
			FunctionId:   rf.FuncId,
			InputData: []*pb.ParameterBinding{{
				Name: "req",
				RpcData: &pb.ParameterBinding_Data{Data: &pb.TypedData{Data: &pb.TypedData_Http{Http: &pb.RpcHttp{
					NullableParams: map[string]*shared.NullableString{
						"id":     nullableStr("42"),
						"tenant": nullableStr("contoso"),
						"null":   nil,
					},
				}}}},
			}},
		}, loaded, app)
	}()

	httpReq, err := http.NewRequest(http.MethodGet, proxy.url+"/api/orders/42", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	httpReq.Header.Set(invocationCorrelationHeader, invocationID)
	httpReq.Header.Set("X-Forwarded-Host", "functions.example.com")
	httpReq.Header.Set("X-Forwarded-Proto", "https")
	httpReq.Header.Set("X-Request-Id", "abc-123")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("client Do: %v", err)
	}
	resp.Body.Close()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for gRPC side")
	}

	select {
	case s := <-got:
		want := seen{
			host:      "functions.example.com",
			scheme:    "https",
			urlHost:   "functions.example.com",
			id:        "42",
			tenant:    "contoso",
			absent:    "",
			requestID: "abc-123",
		}
		if s != want {
			t.Errorf("handler saw %+v, want %+v", s, want)
		}
	default:
		t.Fatal("handler did not run")
	}
}
