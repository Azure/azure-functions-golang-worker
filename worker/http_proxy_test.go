package worker

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// TestHTTPProxy_StreamingResponse verifies that when an HTTP-trigger
// invocation arrives via the HTTP-proxy capability, the user handler runs
// against the live ResponseWriter and chunks are flushed end-to-end.
//
// This is the core invariant of the HttpUri streaming model: the gRPC side
// only carries trigger metadata, and the response body is streamed over
// HTTP without going through any in-memory buffer in the worker.
func TestHTTPProxy_StreamingResponse(t *testing.T) {
	// User handler that flushes incrementally — this is precisely what
	// fails on the legacy buffered ResponseWriterProxy path.
	chunkSent := make(chan struct{}, 5)
	handler := func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Errorf("ResponseWriter does not implement http.Flusher — streaming would not work")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		for i := 0; i < 3; i++ {
			if _, err := w.Write([]byte("chunk\n")); err != nil {
				t.Errorf("write failed: %v", err)
				return
			}
			flusher.Flush()
			chunkSent <- struct{}{}
		}
	}

	app := sdk.FunctionApp()
	app.HTTP("stream", handler)

	proxy := startHTTPProxy(app)
	if proxy == nil {
		t.Fatal("expected HTTP proxy to start")
	}
	t.Cleanup(func() { _ = proxy.shutdown(nil) })

	// Stand in for what the gRPC side normally does: register the loaded
	// function so the coordinator can hand it to the HTTP goroutine.
	var rf *sdk.RegisteredFunction
	app.GetRegisteredFunctions().Range(func(_, value any) bool {
		rf = value.(*sdk.RegisteredFunction)
		return false
	})
	if rf == nil {
		t.Fatal("registered function not found")
	}
	loaded := &LoadedFunction{Function: *rf}

	// Concurrently fire the gRPC side and the HTTP request. They rendezvous
	// in the coordinator regardless of arrival order.
	const invocationID = "test-invocation-1"
	var wg sync.WaitGroup
	var grpcStatus *pb.StatusResult
	var grpcErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		req := &pb.InvocationRequest{
			InvocationId: invocationID,
			FunctionId:   rf.FuncId,
		}
		grpcStatus, grpcErr = proxy.notifyGRPCArrival(req, loaded, app)
	}()

	httpReq, err := http.NewRequest(http.MethodGet, proxy.url+"/api/stream", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	httpReq.Header.Set(invocationCorrelationHeader, invocationID)

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("client Do: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if got, want := strings.Count(string(body), "chunk\n"), 3; got != want {
		t.Errorf("got %d chunks, want %d (body=%q)", got, want, string(body))
	}

	wg.Wait()
	if grpcErr != nil {
		t.Fatalf("notifyGRPCArrival: %v", grpcErr)
	}
	if grpcStatus == nil || grpcStatus.GetStatus() != pb.StatusResult_Success {
		t.Errorf("expected Success, got %v", grpcStatus)
	}
}

// TestHTTPProxy_PanickingHandler verifies a panicking user handler still
// produces a Failure status on the gRPC side so the host doesn't time out.
func TestHTTPProxy_PanickingHandler(t *testing.T) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}

	app := sdk.FunctionApp()
	app.HTTP("explode", handler)

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
	loaded := &LoadedFunction{Function: *rf}

	const invocationID = "test-invocation-panic"
	done := make(chan struct{})
	var status *pb.StatusResult
	go func() {
		defer close(done)
		status, _ = proxy.notifyGRPCArrival(&pb.InvocationRequest{
			InvocationId: invocationID,
			FunctionId:   rf.FuncId,
		}, loaded, app)
	}()

	httpReq, _ := http.NewRequest(http.MethodGet, proxy.url+"/api/explode", nil)
	httpReq.Header.Set(invocationCorrelationHeader, invocationID)
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

	if status == nil || status.GetStatus() != pb.StatusResult_Failure {
		t.Errorf("expected Failure status from panic, got %v", status)
	}
}

// TestStartHTTPProxy_NoHTTPFunctions returns nil when the app has no HTTP
// handlers — preserves the legacy behavior for non-HTTP-only apps.
func TestStartHTTPProxy_NoHTTPFunctions(t *testing.T) {
	app := sdk.FunctionApp()
	app.Timer("nightly", func(_ context.Context, _ bindings.TimerInfo) error { return nil })

	if got := startHTTPProxy(app); got != nil {
		t.Errorf("expected nil proxy when no HTTP functions registered, got %+v", got)
		_ = got.shutdown(nil)
	}
}

// TestStartHTTPProxy_DisabledByEnv verifies the FUNCTIONS_GO_DISABLE_HTTP_PROXY
// opt-out forces the worker back onto the legacy gRPC body path even when
// the app has HTTP triggers.
func TestStartHTTPProxy_DisabledByEnv(t *testing.T) {
	cases := []struct {
		value       string
		wantNil     bool
		description string
	}{
		{"1", true, "1 disables"},
		{"true", true, "true disables"},
		{"TRUE", true, "uppercase TRUE disables"},
		{"yes", true, "any non-empty non-zero value disables"},
		{"0", false, "0 keeps proxy enabled"},
		{"false", false, "false keeps proxy enabled"},
		{"FALSE", false, "uppercase FALSE keeps proxy enabled"},
		{"", false, "empty keeps proxy enabled"},
	}

	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			t.Setenv(disableHTTPProxyEnvVar, tc.value)

			app := sdk.FunctionApp()
			app.HTTP("hello", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			})

			got := startHTTPProxy(app)
			if tc.wantNil && got != nil {
				_ = got.shutdown(nil)
				t.Errorf("%s: expected nil proxy, got %+v", tc.description, got)
			}
			if !tc.wantNil && got == nil {
				t.Errorf("%s: expected proxy to start, got nil", tc.description)
			}
			if got != nil {
				_ = got.shutdown(nil)
			}
		})
	}
}

// TestIsHTTPProxiedInvocation distinguishes a proxied invocation (empty
// RpcHttp from the host) from a legacy gRPC-body invocation.
func TestIsHTTPProxiedInvocation(t *testing.T) {
	proxied := &pb.InvocationRequest{
		InputData: []*pb.ParameterBinding{
			{Name: "req", RpcData: &pb.ParameterBinding_Data{Data: &pb.TypedData{Data: &pb.TypedData_Http{Http: &pb.RpcHttp{}}}}},
		},
	}
	legacy := &pb.InvocationRequest{
		InputData: []*pb.ParameterBinding{
			{Name: "req", RpcData: &pb.ParameterBinding_Data{Data: &pb.TypedData{Data: &pb.TypedData_Http{Http: &pb.RpcHttp{Url: "http://localhost/api/x"}}}}},
		},
	}
	if !isHTTPProxiedInvocation(proxied) {
		t.Error("expected proxied invocation to be detected as HttpUri-proxied")
	}
	if isHTTPProxiedInvocation(legacy) {
		t.Error("expected populated RpcHttp.Url to be detected as legacy gRPC-body path")
	}
}

// TestHTTPProxy_RunsMiddlewareChain validates the fix that ensures
// HTTP-streaming invocations (HttpUri capability) run through the
// App.Compose middleware chain just like gRPC-body invocations. Without
// this, otelfunc.Middleware (and any other CapabilityProvider middleware)
// would silently bypass HTTP-streaming triggers, leaving them
// un-instrumented in production. The bridge is established via the App
// pointer passed to notifyGRPCArrival -> grpcArrival -> invokeHTTPHandler.
func TestHTTPProxy_RunsMiddlewareChain(t *testing.T) {
	// trackingMW counts how many invocations it wraps and stashes the
	// last ctx it saw so we can assert sdk.FromContext is wired through
	// the inner handler.
	var wrapCount int
	type ctxAssertion struct {
		invID  string
		fnName string
	}
	asserted := make(chan ctxAssertion, 1)

	app := sdk.FunctionApp()
	app.HTTP("instrumented", func(w http.ResponseWriter, r *http.Request) {
		ic, ok := sdk.FromContext(r.Context())
		if ok && ic != nil {
			asserted <- ctxAssertion{invID: ic.InvocationID, fnName: ic.FunctionName}
		} else {
			asserted <- ctxAssertion{}
		}
		w.WriteHeader(http.StatusOK)
	})

	// Register a Middleware whose Wrap function increments wrapCount
	// and forwards. Using sdk.MiddlewareFunc keeps the test free of
	// otelfunc-specific machinery.
	app.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, ic *sdk.InvocationContext) error {
			wrapCount++
			return next(ctx, ic)
		}
	}))

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
	loaded := &LoadedFunction{Function: *rf}

	const invocationID = "test-invocation-middleware"
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = proxy.notifyGRPCArrival(&pb.InvocationRequest{
			InvocationId: invocationID,
			FunctionId:   rf.FuncId,
		}, loaded, app)
	}()

	httpReq, _ := http.NewRequest(http.MethodGet, proxy.url+"/api/instrumented", nil)
	httpReq.Header.Set(invocationCorrelationHeader, invocationID)
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

	if wrapCount != 1 {
		t.Errorf("middleware Wrap was called %d times; want 1", wrapCount)
	}
	select {
	case got := <-asserted:
		if got.invID != invocationID {
			t.Errorf("invocation_id from ctx = %q, want %q", got.invID, invocationID)
		}
		if got.fnName != "instrumented" {
			t.Errorf("function_name from ctx = %q, want %q", got.fnName, "instrumented")
		}
	default:
		t.Error("handler did not observe sdk.FromContext (no assertion delivered)")
	}
}
