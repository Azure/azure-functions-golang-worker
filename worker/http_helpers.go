package worker

import (
	"bytes"
	"io/ioutil"
	"net/http"
)

// ResponseWriterProxy is a simple implementation of http.ResponseWriter
// that captures the status code, headers, and body.
type ResponseWriterProxy struct {
	header     http.Header
	statusCode int
	body       *bytes.Buffer
}

func NewResponseWriterProxy() *ResponseWriterProxy {
	return &ResponseWriterProxy{
		header:     make(http.Header),
		statusCode: http.StatusOK, // Default status code
		body:       new(bytes.Buffer),
	}
}

func (rw *ResponseWriterProxy) Header() http.Header {
	return rw.header
}

func (rw *ResponseWriterProxy) Write(b []byte) (int, error) {
	return rw.body.Write(b)
}

func (rw *ResponseWriterProxy) WriteHeader(statusCode int) {
	rw.statusCode = statusCode
}

// Result returns the http.Response captured by the proxy.
func (rw *ResponseWriterProxy) Result() *http.Response {
	return &http.Response{
		StatusCode: rw.statusCode,
		Header:     rw.header,
		Body:       ioutil.NopCloser(bytes.NewReader(rw.body.Bytes())),
	}
}
