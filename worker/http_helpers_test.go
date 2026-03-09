package worker

import (
	"io/ioutil"
	"net/http"
	"testing"
)

func TestNewResponseWriterProxy(t *testing.T) {
	rw := NewResponseWriterProxy()

	if rw == nil {
		t.Fatal("expected non-nil ResponseWriterProxy")
	}
	if rw.statusCode != http.StatusOK {
		t.Errorf("expected default status %d, got %d", http.StatusOK, rw.statusCode)
	}
	if rw.header == nil {
		t.Error("expected non-nil header map")
	}
	if rw.body == nil {
		t.Error("expected non-nil body buffer")
	}
}

func TestResponseWriterProxy_ImplementsInterface(t *testing.T) {
	var _ http.ResponseWriter = NewResponseWriterProxy()
}

func TestResponseWriterProxy_Header(t *testing.T) {
	rw := NewResponseWriterProxy()

	rw.Header().Set("Content-Type", "application/json")
	rw.Header().Set("X-Custom", "value")

	if rw.Header().Get("Content-Type") != "application/json" {
		t.Errorf("expected Content-Type %q, got %q", "application/json", rw.Header().Get("Content-Type"))
	}
	if rw.Header().Get("X-Custom") != "value" {
		t.Errorf("expected X-Custom %q, got %q", "value", rw.Header().Get("X-Custom"))
	}
}

func TestResponseWriterProxy_Write(t *testing.T) {
	rw := NewResponseWriterProxy()

	n, err := rw.Write([]byte("hello "))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 6 {
		t.Errorf("expected 6 bytes written, got %d", n)
	}

	n, err = rw.Write([]byte("world"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 5 {
		t.Errorf("expected 5 bytes written, got %d", n)
	}

	if rw.body.String() != "hello world" {
		t.Errorf("expected body %q, got %q", "hello world", rw.body.String())
	}
}

func TestResponseWriterProxy_WriteHeader(t *testing.T) {
	rw := NewResponseWriterProxy()

	rw.WriteHeader(http.StatusCreated)

	if rw.statusCode != http.StatusCreated {
		t.Errorf("expected status %d, got %d", http.StatusCreated, rw.statusCode)
	}
}

func TestResponseWriterProxy_WriteHeader_NotFound(t *testing.T) {
	rw := NewResponseWriterProxy()

	rw.WriteHeader(http.StatusNotFound)

	if rw.statusCode != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, rw.statusCode)
	}
}

func TestResponseWriterProxy_Result(t *testing.T) {
	rw := NewResponseWriterProxy()
	rw.Header().Set("Content-Type", "text/plain")
	rw.WriteHeader(http.StatusAccepted)
	rw.Write([]byte("accepted"))

	resp := rw.Result()

	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("expected status %d, got %d", http.StatusAccepted, resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "text/plain" {
		t.Errorf("expected Content-Type %q, got %q", "text/plain", resp.Header.Get("Content-Type"))
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if string(body) != "accepted" {
		t.Errorf("expected body %q, got %q", "accepted", string(body))
	}
}

func TestResponseWriterProxy_Result_DefaultStatus(t *testing.T) {
	rw := NewResponseWriterProxy()
	rw.Write([]byte("ok"))

	resp := rw.Result()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected default status %d, got %d", http.StatusOK, resp.StatusCode)
	}
}

func TestResponseWriterProxy_Result_EmptyBody(t *testing.T) {
	rw := NewResponseWriterProxy()
	rw.WriteHeader(http.StatusNoContent)

	resp := rw.Result()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("expected status %d, got %d", http.StatusNoContent, resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if len(body) != 0 {
		t.Errorf("expected empty body, got %q", string(body))
	}
}

func TestResponseWriterProxy_MultipleWrites(t *testing.T) {
	rw := NewResponseWriterProxy()
	rw.Write([]byte("chunk1"))
	rw.Write([]byte("chunk2"))
	rw.Write([]byte("chunk3"))

	resp := rw.Result()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if string(body) != "chunk1chunk2chunk3" {
		t.Errorf("expected body %q, got %q", "chunk1chunk2chunk3", string(body))
	}
}
