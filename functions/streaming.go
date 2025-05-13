package functions

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
)

type HttpResponse struct {
	Body       string
	StatusCode int
	Err        error
}

// Coordinator struct maps invocation IDs to channels
type HttpCoordinator struct {
	mu            sync.Mutex
	responseChans map[string]chan HttpResponse
}

func NewHttpCoordinator() *HttpCoordinator {
	return &HttpCoordinator{
		responseChans: make(map[string]chan HttpResponse),
	}
}

func (hc *HttpCoordinator) SetHttpRequest(invocID string) <-chan HttpResponse {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	ch := make(chan HttpResponse, 1)
	hc.responseChans[invocID] = ch
	return ch
}

func (hc *HttpCoordinator) SetHttpResponse(invocID string, resp HttpResponse) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	ch, ok := hc.responseChans[invocID]
	if !ok {
		log.Printf("No channel found for invocation ID: %s", invocID)
		return
	}

	ch <- resp
	delete(hc.responseChans, invocID)
}

var httpCoordinator = NewHttpCoordinator()

// The host will receive an http invocation. The host will start a proxy forwarding service if the function
// invoked is an httpTrigger. At this point, the worker responds to the forwarded request using the invocation ID
// sent from the host. The worker still has not responded as the host didn't send the invocation request yet.
// Then the worker does respond after receiving the invocation request from the host, and the host will
// gets the response back.
func catchAllHandler(w http.ResponseWriter, r *http.Request) {
	invocId := r.Header.Get(MsInvocationIdHeaderName)
	if invocId == "" {
		http.Error(w, "Missing invocation ID header", http.StatusBadRequest)
		return
	}

	log.Printf("Received HTTP request for invocation %s", invocId)

	responseChan := httpCoordinator.SetHttpRequest(invocId)
	// Simulate some async work (in real code, another goroutine sets the response)
	go func() {
		// Simulate work (e.g. wait for event, db result, etc.)
		// In real use, another part of your app would call SetHttpResponse
		httpCoordinator.SetHttpResponse(invocId, HttpResponse{
			Body:       fmt.Sprintf("Hello from %s", invocId),
			StatusCode: 200,
		})
	}()

	resp := <-responseChan
	if resp.Err != nil {
		http.Error(w, resp.Err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Sending HTTP response for invocation %s", invocId)
	w.WriteHeader(resp.StatusCode)
	fmt.Fprint(w, resp.Body)
}

func StartHttpServer(port int) {
	fmt.Println("Starting HTTP server...")
	http.HandleFunc("/", catchAllHandler)

	fmt.Printf("Starting server at localhost:%d\n", port)
	if err := http.ListenAndServe(fmt.Sprintf("localhost:%d", port), nil); err != nil {
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
