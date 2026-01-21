package worker

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// RpcHttpToRequest converts a protobuf RpcHttp object to a standard http.Request.
func RpcHttpToRequest(rpcHttp *pb.RpcHttp) (*http.Request, error) {
	method := rpcHttp.Method
	if method == "" {
		method = "GET"
	}

	rawUrl := rpcHttp.Url
	u, err := url.Parse(rawUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %v", err)
	}

	var body []byte
	if rpcHttp.Body != nil {
		// RPC body is TypedData
		switch x := rpcHttp.Body.Data.(type) {
		case *pb.TypedData_String_:
			body = []byte(x.String_)
		case *pb.TypedData_Bytes:
			body = x.Bytes
		case *pb.TypedData_Json:
			body = []byte(x.Json)
		}
	}

	req, err := http.NewRequest(method, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create http.Request: %v", err)
	}

	// Populate headers
	for k, v := range rpcHttp.Headers {
		req.Header.Add(k, v)
	}

	// Populate params as query or form data?
	// Params (route params) are usually handled by the host routing,
	// but Query params are in the URL.
	// rpcHttp.Query contains query parameters.

	q := req.URL.Query()
	for k, v := range rpcHttp.Query {
		q.Add(k, v)
	}
	req.URL.RawQuery = q.Encode()

	return req, nil
}
