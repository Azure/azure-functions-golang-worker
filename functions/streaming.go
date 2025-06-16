package functions

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
)

type ContextRef struct {
	Request       *http.Request
	Response      http.ResponseWriter
	RequestReady  chan struct{}
	ResponseReady chan struct{}
}

type HttpCoordinator struct {
	mu       sync.Mutex
	contexts map[string]*ContextRef
}

func NewHttpCoordinator() *HttpCoordinator {
	return &HttpCoordinator{
		contexts: make(map[string]*ContextRef),
	}
}

func (c *HttpCoordinator) ensureContext(invocID string) *ContextRef {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.contexts[invocID]; !exists {
		c.contexts[invocID] = &ContextRef{
			RequestReady:  make(chan struct{}),
			ResponseReady: make(chan struct{}),
		}
	}
	return c.contexts[invocID]
}

func (c *HttpCoordinator) SetHTTPRequest(invocID string, r *http.Request, w http.ResponseWriter) {
	ctx := c.ensureContext(invocID)
	ctx.Request = r
	ctx.Response = w
	close(ctx.RequestReady)
}

func (c *HttpCoordinator) GetHTTPRequest(invocID string) (*http.Request, http.ResponseWriter, error) {
	ctx := c.ensureContext(invocID)

	// Block until the request is ready
	<-ctx.RequestReady

	if ctx.Request == nil {
		return nil, nil, errors.New("no http request")
	}

	return ctx.Request, ctx.Response, nil
}

func (c *HttpCoordinator) NotifyResponseReady(invocID string) {
	ctx := c.ensureContext(invocID)
	close(ctx.ResponseReady)
}

func (c *HttpCoordinator) AwaitResponse(invocID string) {
	ctx := c.ensureContext(invocID)
	<-ctx.ResponseReady
}

var globalCoordinator = NewHttpCoordinator()

// The host will receive an http invocation. The host will start a proxy forwarding service if the function
// invoked is an httpTrigger. At this point, the worker responds to the forwarded request using the invocation ID
// sent from the host. The worker still has not responded as the host didn't send the invocation request yet.
// Then the worker does respond after receiving the invocation request from the host, and the host will
// gets the response back.
func catchAllHandler(w http.ResponseWriter, r *http.Request) {
	invocID := r.Header.Get(MsInvocationIdHeaderName)
	if invocID == "" {
		http.Error(w, "Missing Invocation ID", http.StatusBadRequest)
		return
	}

	fmt.Printf("Received HTTP request for invocation %s\n", invocID)

	globalCoordinator.SetHTTPRequest(invocID, r, w)
	globalCoordinator.AwaitResponse(invocID)
}

func StartHttpServer(port int) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		catchAllHandler(w, r)
	})
	server := &http.Server{
		Addr:    fmt.Sprintf("127.0.0.1:%d", port),
		Handler: handler,
	}

	fmt.Printf("Starting server at http://127.0.0.1:%d\n", port)
	if err := server.ListenAndServe(); err != nil {
		fmt.Println("Server failed:", err)
	}
}

func getUnusedTCPPort() (int, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()

	// Extract the port number
	addr := listener.Addr().(*net.TCPAddr)
	return addr.Port, nil
}

type routeParamKey struct{}

var RouteParamsKey = routeParamKey{}

func SyncRouteParams(r *http.Request, params map[string]string) *http.Request {
	if r == nil {
		panic("http request is nil")
	}
	if params == nil {
		panic("path params are nil")
	}

	// Replace any existing path params
	ctx := context.WithValue(r.Context(), RouteParamsKey, params)
	return r.WithContext(ctx)
}

func GetRouteParam(r *http.Request, key string) (string, bool) {
	params, ok := r.Context().Value(RouteParamsKey).(map[string]string)
	if !ok {
		return "", false
	}
	val, exists := params[key]
	return val, exists
}
