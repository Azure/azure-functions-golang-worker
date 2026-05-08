package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// HTTP proxying / streaming.
//
// When the user app contains HTTP triggers, the worker spawns a small
// http.Server on a loopback port and advertises that URL to the host via the
// "HttpUri" capability in WorkerInitResponse. The host then forwards every
// incoming HTTP request directly to that local server (using YARP), with the
// gRPC InvocationRequest carrying only correlation metadata.
//
// This means the user's net/http handler runs against the *real* live
// http.ResponseWriter from net/http, so http.Flusher, chunked transfer
// encoding, trailers, and streaming bodies all work natively — no buffering
// proxy in the worker.
//
// The HTTP and gRPC arrivals are coordinated by invocation id (header
// "x-ms-invocation-id"). Whichever side arrives first waits in the
// coordinator for the other; the user handler runs on the HTTP goroutine,
// and the result is shipped back to the gRPC goroutine to populate the
// minimal InvocationResponse.

// invocationCorrelationHeader is the header the host adds to forwarded
// requests so the worker can correlate an HTTP request with its
// gRPC InvocationRequest. Matches ScriptConstants.HttpProxyCorrelationHeader
// in the Functions host.
const invocationCorrelationHeader = "x-ms-invocation-id"

// httpInvocationWaitTimeout caps how long either side waits for the other.
// The host's reverse-proxy ActivityTimeout is 240s, so we stay well under
// that for the join, but the HTTP handler itself can run as long as the
// underlying request stays open.
const httpInvocationWaitTimeout = 4 * time.Minute

// pendingHTTPInvocation tracks the two-arrival rendezvous between the gRPC
// invocation goroutine and the HTTP request goroutine for a single
// invocation id.
//
// Buffered channels of size 1 mean that whichever side arrives first never
// blocks on the send, so order of arrival is irrelevant.
type pendingHTTPInvocation struct {
	grpc chan *LoadedFunction  // gRPC side delivers the function to invoke
	done chan *pb.StatusResult // HTTP side delivers the final invocation status
}

// httpProxy is the in-process HTTP server that the host forwards trigger
// requests to. There is at most one per worker process.
type httpProxy struct {
	url      string
	server   *http.Server
	listener net.Listener

	mu      sync.Mutex
	pending map[string]*pendingHTTPInvocation // keyed by invocation id
}

// disableHTTPProxyEnvVar lets users force the worker back onto the legacy
// gRPC-body HTTP path even when the host supports HttpUri proxying. Set to
// "1", "true", or any non-empty value to disable the embedded HTTP server.
//
// Use cases:
//   - Debugging: confirming a regression is/isn't caused by the HTTP-proxy path.
//   - Telemetry tooling that observes request bodies via the gRPC RpcHttp
//     message and would otherwise see an empty body when proxying is on.
//   - Restricted environments where opening a loopback listener is not allowed
//     (forces the fallback explicitly instead of relying on listener failure).
const disableHTTPProxyEnvVar = "FUNCTIONS_GO_DISABLE_HTTP_PROXY"

// startHTTPProxy starts the loopback HTTP server. It returns nil (and logs)
// if the user app registered no HTTP functions, the user disabled HTTP
// proxying via FUNCTIONS_GO_DISABLE_HTTP_PROXY, or the listener could not
// be opened — in any of these cases the worker falls back to the
// gRPC-buffered HTTP path.
func startHTTPProxy(app *sdk.App) *httpProxy {
	if v := os.Getenv(disableHTTPProxyEnvVar); v != "" && v != "0" && !strings.EqualFold(v, "false") {
		log.Printf("HTTP proxy: disabled via %s=%s, using gRPC body for HTTP triggers", disableHTTPProxyEnvVar, v)
		return nil
	}

	if !appHasHTTPFunctions(app) {
		return nil
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Printf("HTTP proxy: failed to open loopback listener, falling back to gRPC body: %v", err)
		return nil
	}

	p := &httpProxy{
		listener: lis,
		pending:  make(map[string]*pendingHTTPInvocation),
		url:      fmt.Sprintf("http://%s", lis.Addr().String()),
	}

	p.server = &http.Server{
		// Catch-all handler — every host-forwarded request lands here
		// regardless of the original URL path; routing happens inside the
		// rendezvous coordinator.
		Handler: http.HandlerFunc(p.handle),
		// No global read/write timeouts — streaming handlers may run for a
		// long time. The host enforces its own ActivityTimeout.
		ReadHeaderTimeout: 30 * time.Second,
	}

	go func() {
		if err := p.server.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP proxy: server stopped: %v", err)
		}
	}()

	log.Printf("HTTP proxy: listening on %s", p.url)
	return p
}

func appHasHTTPFunctions(app *sdk.App) bool {
	found := false
	app.GetRegisteredFunctions().Range(func(_, value any) bool {
		rf := value.(*sdk.RegisteredFunction)
		if _, ok := rf.Func.(http.HandlerFunc); ok {
			found = true
			return false
		}
		// http.HandlerFunc is `func(http.ResponseWriter, *http.Request)` —
		// also catch the bare function type since users may pass it without
		// the alias.
		if isHTTPHandlerFunc(rf.Func) {
			found = true
			return false
		}
		return true
	})
	return found
}

func isHTTPHandlerFunc(f any) bool {
	_, ok := f.(func(http.ResponseWriter, *http.Request))
	return ok
}

// isHTTPHandler reports whether the loaded function is a registered net/http
// handler — i.e. eligible for the HTTP-proxied streaming path.
func isHTTPHandler(lf *LoadedFunction) bool {
	if lf == nil {
		return false
	}
	if _, ok := lf.Function.Func.(http.HandlerFunc); ok {
		return true
	}
	return isHTTPHandlerFunc(lf.Function.Func)
}

// getOrCreatePending returns the rendezvous slot for invocationID, creating
// it if missing. Safe to call from either side concurrently.
func (p *httpProxy) getOrCreatePending(invocationID string) *pendingHTTPInvocation {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pi, ok := p.pending[invocationID]; ok {
		return pi
	}
	pi := &pendingHTTPInvocation{
		grpc: make(chan *LoadedFunction, 1),
		done: make(chan *pb.StatusResult, 1),
	}
	p.pending[invocationID] = pi
	return pi
}

func (p *httpProxy) deletePending(invocationID string) {
	p.mu.Lock()
	delete(p.pending, invocationID)
	p.mu.Unlock()
}

// handle is the HTTP server's catch-all handler. Every host-forwarded
// request lands here, regardless of the original URL path.
func (p *httpProxy) handle(w http.ResponseWriter, r *http.Request) {
	invocationID := r.Header.Get(invocationCorrelationHeader)
	if invocationID == "" {
		http.Error(w, "missing "+invocationCorrelationHeader+" header", http.StatusBadRequest)
		return
	}

	pending := p.getOrCreatePending(invocationID)

	// Wait for the gRPC side to identify the function. The gRPC side fires
	// closely in time to the HTTP forward, but ordering is not guaranteed.
	//
	// Use time.NewTimer + defer t.Stop() rather than time.After so the
	// timer is reclaimed as soon as we leave the select on the success or
	// client-cancel paths. time.After's timer would otherwise stay alive
	// for the full 4-minute timeout even after the invocation completed.
	timeout := time.NewTimer(httpInvocationWaitTimeout)
	defer timeout.Stop()

	var loadedFunc *LoadedFunction
	select {
	case loadedFunc = <-pending.grpc:
	case <-r.Context().Done():
		// Client disconnected (or YARP timed out) before the gRPC trigger
		// arrived. If gRPC arrives later it will block on <-pending.done
		// until the 4-minute hard timeout, so push a Failure status now to
		// unblock it immediately. The buffered channel absorbs the send
		// even if no goroutine is currently receiving.
		log.Printf("HTTP proxy: client disconnected before gRPC trigger arrived for invocation %s", invocationID)
		pending.done <- &pb.StatusResult{
			Status: pb.StatusResult_Failure,
			Exception: &pb.RpcException{
				Message: "client disconnected before invocation could start",
				Source:  "HTTP proxy",
			},
		}
		p.deletePending(invocationID)
		return
	case <-timeout.C:
		log.Printf("HTTP proxy: invocation %s timed out after %v waiting for gRPC trigger", invocationID, httpInvocationWaitTimeout)
		p.deletePending(invocationID)
		http.Error(w, "timed out waiting for gRPC trigger", http.StatusGatewayTimeout)
		return
	}

	// Strip the correlation header so user code doesn't see it.
	r.Header.Del(invocationCorrelationHeader)

	status := &pb.StatusResult{Status: pb.StatusResult_Success}

	// Recover from panics so the gRPC side still gets a response and the
	// host doesn't time out the invocation.
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("HTTP proxy: handler panicked for invocation %s: %v", invocationID, rec)
				status = &pb.StatusResult{
					Status: pb.StatusResult_Failure,
					Exception: &pb.RpcException{
						Message: fmt.Sprintf("%v", rec),
						Source:  "User function",
					},
				}
				// If headers haven't been written, surface the error.
				// If they have, we can't do much; the connection just ends.
				tryWriteError(w, fmt.Sprintf("%v", rec))
			}
		}()

		invokeHTTPHandler(loadedFunc, w, r)
	}()

	pending.done <- status
	p.deletePending(invocationID)
}

// invokeHTTPHandler runs the user's net/http handler. The handler receives
// the live ResponseWriter from the embedded server, so http.Flusher,
// chunked transfer encoding, hijacking, and trailers all work as in any
// standard Go HTTP server.
func invokeHTTPHandler(lf *LoadedFunction, w http.ResponseWriter, r *http.Request) {
	switch h := lf.Function.Func.(type) {
	case http.HandlerFunc:
		h(w, r)
	case func(http.ResponseWriter, *http.Request):
		h(w, r)
	default:
		http.Error(w, "registered handler is not an http.HandlerFunc", http.StatusInternalServerError)
	}
}

// tryWriteError writes an error response only if headers haven't been sent
// yet. If they have, we silently drop the message — the streaming body has
// already started and we can't change the status code anyway.
func tryWriteError(w http.ResponseWriter, msg string) {
	defer func() { _ = recover() }() // http.ResponseWriter.WriteHeader can panic if hijacked
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusInternalServerError)
	_, _ = w.Write([]byte(msg))
}

// notifyGRPCArrival is called from the gRPC dispatcher when an HTTP-trigger
// InvocationRequest is received. It hands the loaded function to the HTTP
// handler goroutine and blocks until the handler reports completion.
//
// Returns the final status to send back in the InvocationResponse.
func (p *httpProxy) notifyGRPCArrival(req *pb.InvocationRequest, lf *LoadedFunction) (*pb.StatusResult, error) {
	invocationID := req.GetInvocationId()
	pending := p.getOrCreatePending(invocationID)

	pending.grpc <- lf

	// time.NewTimer + defer t.Stop() so the timer is reclaimed promptly on
	// the success path; time.After would keep the timer alive for the full
	// 4-minute timeout after every invocation completed.
	timeout := time.NewTimer(httpInvocationWaitTimeout)
	defer timeout.Stop()

	select {
	case status := <-pending.done:
		return status, nil
	case <-timeout.C:
		log.Printf("HTTP proxy: invocation %s timed out after %v waiting for HTTP request", invocationID, httpInvocationWaitTimeout)
		p.deletePending(invocationID)
		return nil, fmt.Errorf("timed out waiting for HTTP request for invocation %s", invocationID)
	}
}

// shutdown gracefully stops the HTTP server. Used by tests for cleanup;
// production termination is handled by process exit.
//
// Passes context.Background() if ctx is nil — http.Server.Shutdown panics
// on a nil context, and tests typically don't have a meaningful ctx to use.
func (p *httpProxy) shutdown(ctx context.Context) error {
	if p == nil || p.server == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return p.server.Shutdown(ctx)
}

// isHTTPProxiedInvocation returns true when the host is forwarding the HTTP
// body via the loopback HTTP server rather than via the gRPC RpcHttp body.
//
// Detection: when proxying, the host sends a near-empty RpcHttp message
// (no method, no url, no body). When the gRPC body path is in use, at
// minimum the URL is populated. We treat "no URL on the trigger input" as
// proof of HTTP proxying.
func isHTTPProxiedInvocation(req *pb.InvocationRequest) bool {
	for _, in := range req.GetInputData() {
		if rpcHTTP := in.GetData().GetHttp(); rpcHTTP != nil {
			if strings.TrimSpace(rpcHTTP.GetUrl()) != "" {
				return false
			}
		}
	}
	return true
}
