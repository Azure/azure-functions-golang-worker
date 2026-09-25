package integration

import (
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"
)

func TestParseMCPResponse(t *testing.T) {
	const result = `{"content":[{"type":"text","text":"unique-echo"}]}`
	const response = `{"jsonrpc":"2.0","id":3,"result":` + result + `}`
	tests := []struct {
		name        string
		contentType string
		body        string
		want        string
		wantError   string
	}{
		{name: "JSON", contentType: "application/json; charset=utf-8", body: response, want: result},
		{name: "SSE", contentType: "text/event-stream", body: "event: message\ndata: " + response + "\n\n", want: result},
		{name: "SSE multiple events", contentType: "text/event-stream", body: ": keepalive\r\n\r\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\r\n\r\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{}}\r\n\r\nid: stream-event\r\nevent: message\r\ndata: " + response + "\r\n\r\n", want: result},
		{name: "SSE multiline data", contentType: "text/event-stream; charset=utf-8", body: "data: {\"jsonrpc\":\"2.0\",\n" + "data: \"id\":3,\"result\":" + result + "}\n\n", want: result},
		{name: "SSE no space after colon", contentType: "text/event-stream", body: "data:" + response + "\n\n", want: result},
		{name: "null result", contentType: "application/json", body: `{"jsonrpc":"2.0","id":3,"result":null}`, want: "null"},
		{name: "malformed JSON", contentType: "application/json", body: "{", wantError: "decode MCP response"},
		{name: "malformed SSE", contentType: "text/event-stream", body: "data: not-json\n\n", wantError: "decode MCP response"},
		{name: "JSON id mismatch", contentType: "application/json", body: `{"jsonrpc":"2.0","id":2,"result":{}}`, wantError: "no response for JSON-RPC id 3"},
		{name: "SSE id mismatch", contentType: "text/event-stream", body: "data: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{}}\n\n", wantError: "no response for JSON-RPC id 3"},
		{name: "string id", contentType: "application/json", body: `{"jsonrpc":"2.0","id":"3","result":{}}`, wantError: "decode MCP response"},
		{name: "JSON RPC error", contentType: "application/json", body: `{"jsonrpc":"2.0","id":3,"error":{"code":-32603,"message":"internal error"}}`, wantError: "JSON-RPC error -32603"},
		{name: "SSE RPC error", contentType: "text/event-stream", body: "data: {\"jsonrpc\":\"2.0\",\"id\":3,\"error\":{\"code\":-32603,\"message\":\"internal error\"}}\n\n", wantError: "JSON-RPC error -32603"},
		{name: "missing id", contentType: "application/json", body: `{"jsonrpc":"2.0","result":{}}`, wantError: "missing JSON-RPC id"},
		{name: "wrong version", contentType: "application/json", body: `{"jsonrpc":"1.0","id":3,"result":{}}`, wantError: "JSON-RPC version"},
		{name: "missing result", contentType: "application/json", body: `{"jsonrpc":"2.0","id":3}`, wantError: "missing result"},
		{name: "result and error", contentType: "application/json", body: `{"jsonrpc":"2.0","id":3,"result":{},"error":{"code":-32603}}`, wantError: "both result and error"},
		{name: "trailing JSON", contentType: "application/json", body: response + response, wantError: "decode MCP response"},
		{name: "empty stream", contentType: "text/event-stream", body: ": keepalive\n\n", wantError: "no response for JSON-RPC id 3"},
		{name: "unsupported content type", contentType: "text/html", body: response, wantError: "MCP content type"},
		{name: "oversized JSON", contentType: "application/json", body: strings.Repeat(" ", (1<<20)+1), wantError: "exceeds"},
		{name: "oversized SSE", contentType: "text/event-stream", body: strings.Repeat(": keepalive\n\n", 100000), wantError: "exceeds"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMCPResponse(strings.NewReader(tt.body), tt.contentType, 3)
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("parseMCPResponse() error = %v, want %q", err, tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseMCPResponse() error = %v", err)
			}
			if !json.Valid(got) || string(got) != tt.want {
				t.Fatalf("parseMCPResponse() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestParseMCPResponseOpenStream(t *testing.T) {
	reader, writer := io.Pipe()
	t.Cleanup(func() {
		reader.Close()
		writer.Close()
	})
	go func() {
		// Deliberately leave the stream open after the complete response event.
		_, _ = io.WriteString(writer, "data: {\"jsonrpc\":\"2.0\",\"id\":3,\"result\":\"echo\"}\n\n")
	}()
	type response struct {
		result json.RawMessage
		err    error
	}
	done := make(chan response, 1)
	go func() {
		result, err := parseMCPResponse(reader, "text/event-stream", 3)
		done <- response{result, err}
	}()
	select {
	case got := <-done:
		if got.err != nil || string(got.result) != `"echo"` {
			t.Fatalf("open stream response = %s, error = %v", got.result, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parser waited for EOF instead of returning the complete SSE response")
	}
}
