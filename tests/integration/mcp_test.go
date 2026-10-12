package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"testing"
	"time"
)

type mcpTestClient struct {
	endpoint  string
	sessionID string
	client    *http.Client
}

// An id of zero sends a notification, which has no JSON-RPC id or result.
func (c *mcpTestClient) request(t *testing.T, id int, method string, params any) json.RawMessage {
	t.Helper()
	message := map[string]any{"jsonrpc": "2.0", "method": method, "params": params}
	if id != 0 {
		message["id"] = id
	}
	body, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("encode %s request: %v", method, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create %s request: %v", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", "2025-03-26")
	if c.sessionID != "" {
		req.Header.Set("Mcp-Session-Id", c.sessionID)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		t.Fatalf("send %s request: %v", method, err)
	}
	defer resp.Body.Close()
	if sessionID := resp.Header.Get("Mcp-Session-Id"); sessionID != "" {
		c.sessionID = sessionID
	}
	if id == 0 {
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("%s HTTP status = %d, want 202", method, resp.StatusCode)
		}
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s HTTP status = %d, want 200", method, resp.StatusCode)
	}
	result, err := parseMCPResponse(resp.Body, resp.Header.Get("Content-Type"), id)
	if err != nil {
		t.Fatalf("read %s response: %v", method, err)
	}
	return result
}

// parseMCPResponse accepts JSON or SSE, selecting the requested JSON-RPC id.
// SSE is read incrementally so an open stream need not close after its response.
func parseMCPResponse(reader io.Reader, contentType string, id int) (json.RawMessage, error) {
	const maxBytes = 1 << 20
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || (mediaType != "application/json" && mediaType != "text/event-stream") {
		return nil, fmt.Errorf("unsupported MCP content type %q", contentType)
	}
	decode := func(data []byte) (json.RawMessage, error) {
		var message struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      *int            `json:"id"`
			Method  string          `json:"method"`
			Result  json.RawMessage `json:"result"`
			Error   *struct {
				Code int `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(data, &message); err != nil {
			return nil, fmt.Errorf("decode MCP response: %w", err)
		}
		if message.JSONRPC != "2.0" {
			return nil, fmt.Errorf("unexpected JSON-RPC version %q", message.JSONRPC)
		}
		if message.ID == nil {
			if message.Method != "" {
				return nil, nil
			}
			return nil, fmt.Errorf("missing JSON-RPC id")
		}
		if *message.ID != id {
			return nil, nil
		}
		if message.Error != nil {
			if message.Result != nil {
				return nil, fmt.Errorf("MCP response contains both result and error")
			}
			return nil, fmt.Errorf("JSON-RPC error %d", message.Error.Code)
		}
		if message.Result == nil {
			return nil, fmt.Errorf("MCP response missing result")
		}
		return message.Result, nil
	}
	limited := &io.LimitedReader{R: reader, N: maxBytes + 1}
	tooLarge := fmt.Errorf("MCP response exceeds %d bytes", maxBytes)
	if mediaType == "application/json" {
		data, err := io.ReadAll(limited)
		if err != nil {
			return nil, fmt.Errorf("read MCP response: %w", err)
		}
		if len(data) > maxBytes {
			return nil, tooLarge
		}
		if result, err := decode(data); result != nil || err != nil {
			return result, err
		}
	} else {
		scanner := bufio.NewScanner(limited)
		scanner.Buffer(make([]byte, 4096), maxBytes+1)
		var data []string
		for scanner.Scan() {
			if limited.N == 0 {
				return nil, tooLarge
			}
			line := scanner.Text()
			if line == "" && len(data) > 0 {
				if result, err := decode([]byte(strings.Join(data, "\n"))); result != nil || err != nil {
					return result, err
				}
				data = nil
			} else if value, ok := strings.CutPrefix(line, "data:"); ok {
				data = append(data, strings.TrimPrefix(value, " "))
			}
		}
		if limited.N == 0 {
			return nil, tooLarge
		}
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("read MCP event stream: %w", err)
		}
	}
	return nil, fmt.Errorf("no response for JSON-RPC id %d", id)
}
