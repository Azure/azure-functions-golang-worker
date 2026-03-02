package sdk

import (
	"context"
	"net/http"
)

// Context is the runtime context for the function execution.
type Context struct {
	context.Context
	FunctionID   string
	InvocationID string
	Log          Logger
}

type Logger interface {
	Log(p []byte) (n int, err error)
}

// HttpRequest mirrors the standard http.Request but for Azure Functions context.
// In a full implementation, this might wrap the standard request or be the standard request.
type HttpRequest struct {
	*http.Request
}

// HttpResponse is a placeholder for return values.
type HttpResponse struct {
	Body       []byte
	StatusCode int
	Header     http.Header
}
