package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	grpc chan grpcArrival // gRPC side delivers the function + invocation request
	done chan httpResult  // HTTP side delivers the final status + the post-invocation mc
}

// httpResult is the payload the HTTP-side goroutine returns to the gRPC
// dispatcher once the user handler has finished running. The gRPC side
// uses mc to build the InvocationResponse the same way the gRPC-body
// path does (reading e.g. mc.OutboundTraceAttributes()).
//
// Ownership: the HTTP goroutine exclusively mutates mc up to the channel
// send; the receive is the handoff. After the send, mc is read-only to
// the gRPC side.
//
// mc is non-nil for every code path that paired with a gRPC arrival,
// including the user-handler-panicked path (mc is constructed before
// invokeHTTPHandler runs, so partial mutations made before the panic
// still reach the gRPC side). mc is nil only on the early-return
// "client disconnected before gRPC arrival" branch; the gRPC side
// nil-checks as a safety belt.
type httpResult struct {
	status *pb.StatusResult
	mc     *sdk.MiddlewareContext
}

// grpcArrival carries everything the HTTP handler needs from the gRPC
// side: the loaded function, the originating InvocationRequest (used to
// build the per-invocation sdk.InvocationContext), the App reference
// (used to compose the registered Middleware chain around the handler),
// and a logger for system-level events.
//
// This is the integration point that ensures HTTP-streaming invocations
// run through the same App.Compose chain as gRPC-body invocations, so
// middleware like otelfunc wraps both paths uniformly.
type grpcArrival struct {
	fn           *LoadedFunction
	req          *pb.InvocationRequest
	app          *sdk.App
	systemLogger *slog.Logger
}

// httpProxy is the in-process HTTP server that the host forwards trigger
// requests to. There is at most one per worker process.
type httpProxy struct {
	url          string
	server       *http.Server
	listener     net.Listener
	systemLogger *slog.Logger

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
		slog.InfoContext(context.Background(), "HTTP proxy: disabled, using gRPC body for HTTP triggers",
			slog.String("env_var", disableHTTPProxyEnvVar),
			slog.String("env_value", v),
		)
		return nil
	}

	if !appHasHTTPFunctions(app) {
		return nil
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		slog.ErrorContext(context.Background(), "HTTP proxy: failed to open loopback listener, falling back to gRPC body",
			slog.Any("err", err),
		)
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
			slog.ErrorContext(context.Background(), "HTTP proxy: server stopped",
				slog.Any("err", err),
			)
		}
	}()

	slog.InfoContext(context.Background(), "HTTP proxy: listening on loopback",
		slog.String("url", p.url),
	)
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
		grpc: make(chan grpcArrival, 1),
		done: make(chan httpResult, 1),
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

	var arrival grpcArrival
	select {
	case arrival = <-pending.grpc:
	case <-r.Context().Done():
		// Client disconnected (or YARP timed out) before the gRPC trigger
		// arrived. If gRPC arrives later it will block on <-pending.done
		// until the 4-minute hard timeout, so push a Failure status now to
		// unblock it immediately. The buffered channel absorbs the send
		// even if no goroutine is currently receiving.
		//
		// ic is nil here because no user handler ran -- nothing for the
		// gRPC side to copy onto the InvocationResponse.
		if arrival.systemLogger != nil {
			arrival.systemLogger.LogAttrs(context.Background(), slog.LevelWarn, "HTTP proxy: client disconnected before gRPC trigger arrived",
				slog.String("invocation_id", invocationID),
			)
		}
		pending.done <- httpResult{
			status: &pb.StatusResult{
				Status: pb.StatusResult_Failure,
				Exception: &pb.RpcException{
					Message: "client disconnected before invocation could start",
					Source:  "HTTP proxy",
				},
			},
		}
		p.deletePending(invocationID)
		return
	case <-timeout.C:
		if arrival.systemLogger != nil {
			arrival.systemLogger.LogAttrs(context.Background(), slog.LevelWarn, "HTTP proxy: gRPC trigger did not arrive before timeout",
				slog.String("invocation_id", invocationID),
				slog.Duration("timeout", httpInvocationWaitTimeout),
			)
		}
		p.deletePending(invocationID)
		http.Error(w, "timed out waiting for gRPC trigger", http.StatusGatewayTimeout)
		return
	}

	// Strip the correlation header so user code doesn't see it.
	r.Header.Del(invocationCorrelationHeader)

	// Build the per-invocation MiddlewareContext up here (not inside
	// invokeHTTPHandler) so that if the user handler panics, the mc we've
	// already populated up to the point of the panic still reaches the
	// gRPC side via the rendezvous channel. The gRPC-body path has this
	// property naturally because it builds the carrier locally and runs
	// the handler on the same goroutine; we replicate the property here by
	// hoisting construction above the invocation call.
	mc := buildMiddlewareContext(arrival.req, &arrival.fn.Function)

	// Surface trigger input and auxiliary input bindings onto the carrier so
	// middleware sees the same data as on the gRPC-body path. This is what
	// makes input bindings (for example, the durable client binding that
	// delivers the host's durable gRPC endpoint) available to a starter
	// function reached over the HTTP-streaming path.
	if raw := extractTriggerInputBytes(arrival.req, arrival.fn); raw != nil {
		mc.SetInputBytes(raw)
	}
	populateBindingInputs(arrival.req, arrival.fn, mc)

	// Run the user handler via the shared invocation pipeline so both the
	// HTTP-streaming path and the gRPC-body path produce identical
	// StatusResult shapes for panics and errors. runUserInvocation
	// composes the middleware chain, executes it, and converts any panic
	// into a recovered value the caller can inspect.
	recovered, stack, invokeErr := invokeHTTPHandler(arrival, mc, w, r)
	if recovered != nil {
		if arrival.systemLogger != nil {
			arrival.systemLogger.LogAttrs(context.Background(), slog.LevelError, "HTTP proxy: handler panicked",
				slog.String("invocation_id", invocationID),
				slog.Any("panic", recovered),
				slog.String("stack", stack),
			)
		}
		// If headers haven't been written, surface the error.
		// If they have, we can't do much; the connection just ends.
		tryWriteError(w, fmt.Sprintf("%v", recovered))
	} else if invokeErr != nil {
		// Middleware short-circuit (e.g. auth) returned an error. HTTP
		// responses are owned by the user handler / middleware, so we only
		// log for diagnostics — InvocationResponse status comes from the
		// rendezvous channel back to the gRPC side.
		if arrival.systemLogger != nil {
			arrival.systemLogger.LogAttrs(context.Background(), slog.LevelError, "HTTP proxy: middleware chain returned error",
				slog.String("invocation_id", invocationID),
				slog.Any("err", invokeErr),
			)
		}
	}
	status := statusFromInvocation(recovered, stack, invokeErr)

	pending.done <- httpResult{status: status, mc: mc}
	p.deletePending(invocationID)
}

// invokeHTTPHandler runs the user's net/http handler through the shared
// invocation pipeline. The handler receives the live ResponseWriter from
// the embedded server, so http.Flusher, chunked transfer encoding,
// hijacking, and trailers all work as in any standard Go HTTP server.
//
// mc is built by the caller so partial mutations the user handler made
// survive a panic. We attach it to r.Context() and run the call through
// [runUserInvocation], which composes arrival.app's middleware chain and
// recovers panics — keeping panic/error semantics identical to the
// gRPC-body path in [handleInvocationRequest].
func invokeHTTPHandler(arrival grpcArrival, mc *sdk.MiddlewareContext, w http.ResponseWriter, r *http.Request) (recovered any, stack string, err error) {
	handler, ok := arrival.fn.Function.Func.(func(http.ResponseWriter, *http.Request))
	if !ok {
		if h, ok2 := arrival.fn.Function.Func.(http.HandlerFunc); ok2 {
			handler = h
		} else {
			http.Error(w, "registered handler is not an http.HandlerFunc", http.StatusInternalServerError)
			return nil, "", nil
		}
	}

	ctx := sdk.ContextWithMiddleware(r.Context(), mc)

	// inner is the innermost Handler the middleware chain wraps; it
	// forwards the (possibly enriched) ctx into the request. HTTP
	// handlers surface errors via response status, so inner always
	// returns nil — only middleware can produce a non-nil error.
	inner := func(ctx context.Context, _ *sdk.MiddlewareContext) error {
		handler(w, r.WithContext(ctx))
		return nil
	}

	return runUserInvocation(ctx, mc, arrival.app, inner)
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
// InvocationRequest is received. It hands the loaded function plus the
// request to the HTTP handler goroutine and blocks until the handler
// reports completion.
//
// Returns the final status plus the [sdk.MiddlewareContext] the HTTP
// goroutine populated. mc is nil only on the early-return "client
// disconnected before gRPC arrival" branch in [handle]; the caller must
// nil-check before dereferencing.
func (p *httpProxy) notifyGRPCArrival(req *pb.InvocationRequest, lf *LoadedFunction, app *sdk.App) (*pb.StatusResult, *sdk.MiddlewareContext, error) {
	invocationID := req.GetInvocationId()
	pending := p.getOrCreatePending(invocationID)

	pending.grpc <- grpcArrival{fn: lf, req: req, app: app, systemLogger: p.systemLogger}

	// time.NewTimer + defer t.Stop() so the timer is reclaimed promptly on
	// the success path; time.After would keep the timer alive for the full
	// 4-minute timeout after every invocation completed.
	timeout := time.NewTimer(httpInvocationWaitTimeout)
	defer timeout.Stop()

	select {
	case result := <-pending.done:
		return result.status, result.mc, nil
	case <-timeout.C:
		if p.systemLogger != nil {
			p.systemLogger.LogAttrs(context.Background(), slog.LevelError, "HTTP proxy: invocation timed out waiting for HTTP request",
				slog.String("invocation_id", invocationID),
				slog.Duration("timeout", httpInvocationWaitTimeout),
			)
		}
		p.deletePending(invocationID)
		return nil, nil, fmt.Errorf("timed out waiting for HTTP request for invocation %s", invocationID)
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
