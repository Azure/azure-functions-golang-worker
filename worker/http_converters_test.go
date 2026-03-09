package worker

import (
	"io/ioutil"
	"testing"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

func TestRpcHttpToRequest_BasicGET(t *testing.T) {
	rpcHttp := &pb.RpcHttp{
		Method: "GET",
		Url:    "http://localhost/api/hello",
	}

	req, err := RpcHttpToRequest(rpcHttp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Method != "GET" {
		t.Errorf("expected method GET, got %s", req.Method)
	}
	if req.URL.Path != "/api/hello" {
		t.Errorf("expected path /api/hello, got %s", req.URL.Path)
	}
}

func TestRpcHttpToRequest_DefaultMethod(t *testing.T) {
	rpcHttp := &pb.RpcHttp{
		Url: "http://localhost/api/test",
	}

	req, err := RpcHttpToRequest(rpcHttp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Method != "GET" {
		t.Errorf("expected default method GET, got %s", req.Method)
	}
}

func TestRpcHttpToRequest_POST_StringBody(t *testing.T) {
	rpcHttp := &pb.RpcHttp{
		Method: "POST",
		Url:    "http://localhost/api/submit",
		Body: &pb.TypedData{
			Data: &pb.TypedData_String_{String_: "form data"},
		},
	}

	req, err := RpcHttpToRequest(rpcHttp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if req.Method != "POST" {
		t.Errorf("expected method POST, got %s", req.Method)
	}

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if string(body) != "form data" {
		t.Errorf("expected body %q, got %q", "form data", string(body))
	}
}

func TestRpcHttpToRequest_POST_BytesBody(t *testing.T) {
	rpcHttp := &pb.RpcHttp{
		Method: "POST",
		Url:    "http://localhost/api/upload",
		Body: &pb.TypedData{
			Data: &pb.TypedData_Bytes{Bytes: []byte("binary payload")},
		},
	}

	req, err := RpcHttpToRequest(rpcHttp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if string(body) != "binary payload" {
		t.Errorf("expected body %q, got %q", "binary payload", string(body))
	}
}

func TestRpcHttpToRequest_POST_JSONBody(t *testing.T) {
	rpcHttp := &pb.RpcHttp{
		Method: "POST",
		Url:    "http://localhost/api/json",
		Body: &pb.TypedData{
			Data: &pb.TypedData_Json{Json: `{"key":"value"}`},
		},
	}

	req, err := RpcHttpToRequest(rpcHttp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if string(body) != `{"key":"value"}` {
		t.Errorf("expected body %q, got %q", `{"key":"value"}`, string(body))
	}
}

func TestRpcHttpToRequest_Headers(t *testing.T) {
	rpcHttp := &pb.RpcHttp{
		Method: "GET",
		Url:    "http://localhost/api/headers",
		Headers: map[string]string{
			"Content-Type":  "application/json",
			"Authorization": "Bearer token123",
			"X-Custom":      "custom-value",
		},
	}

	req, err := RpcHttpToRequest(rpcHttp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tests := []struct {
		header string
		want   string
	}{
		{"Content-Type", "application/json"},
		{"Authorization", "Bearer token123"},
		{"X-Custom", "custom-value"},
	}

	for _, tc := range tests {
		got := req.Header.Get(tc.header)
		if got != tc.want {
			t.Errorf("header %q: expected %q, got %q", tc.header, tc.want, got)
		}
	}
}

func TestRpcHttpToRequest_QueryParams(t *testing.T) {
	rpcHttp := &pb.RpcHttp{
		Method: "GET",
		Url:    "http://localhost/api/search",
		Query: map[string]string{
			"q":    "golang",
			"page": "1",
		},
	}

	req, err := RpcHttpToRequest(rpcHttp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	q := req.URL.Query()
	if q.Get("q") != "golang" {
		t.Errorf("expected query param q=%q, got %q", "golang", q.Get("q"))
	}
	if q.Get("page") != "1" {
		t.Errorf("expected query param page=%q, got %q", "1", q.Get("page"))
	}
}

func TestRpcHttpToRequest_NilBody(t *testing.T) {
	rpcHttp := &pb.RpcHttp{
		Method: "GET",
		Url:    "http://localhost/api/nobody",
	}

	req, err := RpcHttpToRequest(rpcHttp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if len(body) != 0 {
		t.Errorf("expected empty body, got %q", string(body))
	}
}

func TestRpcHttpToRequest_AllHTTPMethods(t *testing.T) {
	methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			rpcHttp := &pb.RpcHttp{
				Method: method,
				Url:    "http://localhost/api/test",
			}

			req, err := RpcHttpToRequest(rpcHttp)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if req.Method != method {
				t.Errorf("expected method %s, got %s", method, req.Method)
			}
		})
	}
}
