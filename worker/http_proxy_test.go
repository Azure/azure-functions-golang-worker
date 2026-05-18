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
		grpcStatus, _, grpcErr = proxy.notifyGRPCArrival(req, loaded, app)
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
		status, _, _ = proxy.notifyGRPCArrival(&pb.InvocationRequest{
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
		return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
			wrapCount++
			return next(ctx, mc)
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
		_, _, _ = proxy.notifyGRPCArrival(&pb.InvocationRequest{
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

// TestHTTPProxy_PropagatesIcToGRPCSide asserts that the
// sdk.MiddlewareContext the HTTP goroutine populates is handed across
// the rendezvous channel to the gRPC dispatcher. The gRPC side uses
// the returned mc to build the InvocationResponse the same way the
// gRPC-body path does (reading invocation-output fields directly off
// the mc). This is the structural fix: as long as the mc crosses the
// boundary, the gRPC dispatcher can pick up any user-mutable field
// without per-field plumbing.
//
// The test exercises [sdk.MiddlewareContext.SetOutboundTraceAttribute]
// because that's the first such writer shipped, but the contract being
// asserted is broader: "whatever the user / middleware wrote to the
// mc on the HTTP goroutine is observable on the gRPC side."
func TestHTTPProxy_PropagatesIcToGRPCSide(t *testing.T) {
	app := sdk.FunctionApp()
	app.HTTP("attribute-setter", func(w http.ResponseWriter, r *http.Request) {
		mc, ok := sdk.MiddlewareContextFrom(r.Context())
		if !ok || mc == nil {
			t.Errorf("MiddlewareContext missing from request context")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// Simulate the middleware writer path: SetOutboundTraceAttribute
		// lazy-allocates the map and stores each entry. Both calls must
		// survive the HTTP -> gRPC handoff.
		mc.SetOutboundTraceAttribute("test.kind", "outbound-trace-attr")
		mc.SetOutboundTraceAttribute("test.fn", mc.FunctionName)
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
	loaded := &LoadedFunction{Function: *rf}

	const invocationID = "test-invocation-ic-roundtrip"
	done := make(chan struct{})
	var gotStatus *pb.StatusResult
	var gotMC *sdk.MiddlewareContext
	go func() {
		defer close(done)
		gotStatus, gotMC, _ = proxy.notifyGRPCArrival(&pb.InvocationRequest{
			InvocationId: invocationID,
			FunctionId:   rf.FuncId,
		}, loaded, app)
	}()

	httpReq, _ := http.NewRequest(http.MethodGet, proxy.url+"/api/attribute-setter", nil)
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

	if gotStatus == nil || gotStatus.GetStatus() != pb.StatusResult_Success {
		t.Errorf("expected Success status, got %v", gotStatus)
	}
	if gotMC == nil {
		t.Fatal("expected non-nil MiddlewareContext from notifyGRPCArrival; got nil")
	}
	// The MiddlewareContext that comes out of the rendezvous is the
	// same one the user handler mutated -- the gRPC side can read any
	// field off it that the gRPC-body path would.
	if got, want := gotMC.InvocationID, invocationID; got != want {
		t.Errorf("mc.InvocationID = %q, want %q", got, want)
	}
	if got, want := gotMC.FunctionName, "attribute-setter"; got != want {
		t.Errorf("mc.FunctionName = %q, want %q", got, want)
	}
	outbound := gotMC.OutboundTraceAttributes()
	if got, want := outbound["test.kind"], "outbound-trace-attr"; got != want {
		t.Errorf("mc.OutboundTraceAttributes()[\"test.kind\"] = %q, want %q (full map: %v)",
			got, want, outbound)
	}
	if got, want := outbound["test.fn"], "attribute-setter"; got != want {
		t.Errorf("mc.OutboundTraceAttributes()[\"test.fn\"] = %q, want %q (full map: %v)",
			got, want, outbound)
	}
}

// TestHTTPProxy_NoIcMutationsAreVisible asserts that when neither the
// handler nor any middleware mutates the mc, the gRPC side still
// receives a valid mc (constructed from the InvocationRequest) and any
// invocation-output fields on it are at their zero values. The gRPC
// dispatcher then emits an InvocationResponse with no propagated
// invocation-output fields, matching the gRPC-body path's behavior.
func TestHTTPProxy_NoIcMutationsAreVisible(t *testing.T) {
	app := sdk.FunctionApp()
	app.HTTP("noop", func(w http.ResponseWriter, r *http.Request) {
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
	loaded := &LoadedFunction{Function: *rf}

	const invocationID = "test-invocation-no-ic-mutation"
	done := make(chan struct{})
	var gotMC *sdk.MiddlewareContext
	go func() {
		defer close(done)
		_, gotMC, _ = proxy.notifyGRPCArrival(&pb.InvocationRequest{
			InvocationId: invocationID,
			FunctionId:   rf.FuncId,
		}, loaded, app)
	}()

	httpReq, _ := http.NewRequest(http.MethodGet, proxy.url+"/api/noop", nil)
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

	if gotMC == nil {
		t.Fatal("expected non-nil MiddlewareContext even when handler does not mutate it; got nil")
	}
	if outbound := gotMC.OutboundTraceAttributes(); outbound != nil {
		t.Errorf("expected nil OutboundTraceAttributes when handler does not touch the map; got %v",
			outbound)
	}
}

// TestHTTPProxy_IcSurvivesHandlerPanic asserts that mutations the user
// handler made to the mc BEFORE panicking still reach the gRPC side --
// matching the gRPC-body path's behavior. This locks in the property
// that motivated hoisting mc construction above the panic-recover
// block in [httpProxy.handle]: if mc were built inside
// invokeHTTPHandler, a panic would unwind the stack before the return,
// the outer mc would stay nil, and any partial mutations the handler
// made would be silently dropped on the streaming path while still
// being propagated on the gRPC-body path. With mc hoisted, the handoff
// is symmetric across paths.
func TestHTTPProxy_IcSurvivesHandlerPanic(t *testing.T) {
	app := sdk.FunctionApp()
	app.HTTP("panic-after-mutate", func(w http.ResponseWriter, r *http.Request) {
		mc, _ := sdk.MiddlewareContextFrom(r.Context())
		// Mutate the mc, then panic. The mutation must survive the
		// panic and be visible on the gRPC side.
		mc.SetOutboundTraceAttribute("before.panic", "kept")
		panic("intentional panic after mc mutation")
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
	loaded := &LoadedFunction{Function: *rf}

	const invocationID = "test-invocation-panic-survivor"
	done := make(chan struct{})
	var gotStatus *pb.StatusResult
	var gotMC *sdk.MiddlewareContext
	go func() {
		defer close(done)
		gotStatus, gotMC, _ = proxy.notifyGRPCArrival(&pb.InvocationRequest{
			InvocationId: invocationID,
			FunctionId:   rf.FuncId,
		}, loaded, app)
	}()

	httpReq, _ := http.NewRequest(http.MethodGet, proxy.url+"/api/panic-after-mutate", nil)
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

	// Status must be Failure (user code panicked) but mc must still be
	// non-nil and carry the pre-panic mutation.
	if gotStatus == nil || gotStatus.GetStatus() != pb.StatusResult_Failure {
		t.Errorf("expected Failure status from panicking handler, got %v", gotStatus)
	}
	if gotMC == nil {
		t.Fatal("expected non-nil MiddlewareContext even when handler panics; got nil")
	}
	outbound := gotMC.OutboundTraceAttributes()
	if got, want := outbound["before.panic"], "kept"; got != want {
		t.Errorf("OutboundTraceAttributes()[\"before.panic\"] = %q, want %q (full map: %v)",
			got, want, outbound)
	}
}
