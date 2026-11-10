package worker

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
	"github.com/azure/azure-functions-golang-worker/worker/proto/shared"
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

	applyRequestHeaders(req.Header, rpcHttp)
	mergeQuery(req.URL, rpcHttp)
	applyRouteParams(req, rpcHttp)

	return req, nil
}

// encodeResponseHeaders flattens the handler's response headers for the RPC
// map. Repeated values are joined with ", " per RFC 9110, so none are dropped.
// Set-Cookie is excluded because it cannot be joined safely, and is sent as
// structured cookies instead (see encodeCookies).
func encodeResponseHeaders(h http.Header) map[string]string {
	headers := make(map[string]string, len(h))
	for k, v := range h {
		if len(v) == 0 || k == "Set-Cookie" {
			continue
		}
		headers[k] = strings.Join(v, ", ")
	}
	return headers
}

// encodeCookies converts parsed Set-Cookie headers into the RPC cookie shape.
// Attributes the handler did not set stay unset so the host applies its own
// defaults. The RPC cookie has no field for Partitioned or unknown
// attributes, so those are not forwarded.
func encodeCookies(cookies []*http.Cookie) []*pb.RpcHttpCookie {
	if len(cookies) == 0 {
		return nil
	}

	out := make([]*pb.RpcHttpCookie, 0, len(cookies))
	for _, c := range cookies {
		rc := &pb.RpcHttpCookie{Name: c.Name, Value: c.Value}

		if c.Domain != "" {
			rc.Domain = &shared.NullableString{String_: &shared.NullableString_Value{Value: c.Domain}}
		}
		if c.Path != "" {
			rc.Path = &shared.NullableString{String_: &shared.NullableString_Value{Value: c.Path}}
		}
		if !c.Expires.IsZero() {
			rc.Expires = &shared.NullableTimestamp{Timestamp: &shared.NullableTimestamp_Value{Value: timestamppb.New(c.Expires.UTC())}}
		}
		if c.Secure {
			rc.Secure = &shared.NullableBool{Bool: &shared.NullableBool_Value{Value: true}}
		}
		if c.HttpOnly {
			rc.HttpOnly = &shared.NullableBool{Bool: &shared.NullableBool_Value{Value: true}}
		}
		// net/http reports an absent Max-Age as 0 and "Max-Age=0" as -1.
		if c.MaxAge != 0 {
			rc.MaxAge = &shared.NullableDouble{Double: &shared.NullableDouble_Value{Value: float64(max(c.MaxAge, 0))}}
		}

		switch c.SameSite {
		case http.SameSiteLaxMode:
			rc.SameSite = pb.RpcHttpCookie_Lax
		case http.SameSiteStrictMode:
			rc.SameSite = pb.RpcHttpCookie_Strict
		case http.SameSiteNoneMode:
			rc.SameSite = pb.RpcHttpCookie_ExplicitNone
		}

		out = append(out, rc)
	}
	return out
}

// forEachHTTPValue visits the entries of an RpcHttp map pair. The host sends
// the nullable map when UseNullableValueDictionaryForHttp is advertised, which
// this worker always does, and leaves the plain map empty. The plain map is
// visited first so a nullable entry wins on a duplicate key. Null entries are
// skipped, while explicitly empty values are kept.
func forEachHTTPValue(plain map[string]string, nullable map[string]*shared.NullableString, fn func(k, v string)) {
	for k, v := range plain {
		fn(k, v)
	}
	for k, v := range nullable {
		if v == nil || v.GetString_() == nil {
			continue
		}
		fn(k, v.GetValue())
	}
}

// applyRequestHeaders copies the request headers onto h. The host sends
// multi-valued headers already joined into a single value.
func applyRequestHeaders(h http.Header, rpcHttp *pb.RpcHttp) {
	forEachHTTPValue(rpcHttp.GetHeaders(), rpcHttp.GetNullableHeaders(), h.Set)
}

// mergeQuery adds query entries that the URL does not already carry. The URL
// is the source of truth for the query string, so entries already present are
// never duplicated and the original encoding is preserved.
func mergeQuery(u *url.URL, rpcHttp *pb.RpcHttp) {
	entries := map[string]string{}
	forEachHTTPValue(rpcHttp.GetQuery(), rpcHttp.GetNullableQuery(), func(k, v string) {
		entries[k] = v
	})
	if len(entries) == 0 {
		return
	}

	existing := u.Query()
	missing := url.Values{}
	for k, v := range entries {
		if !existing.Has(k) {
			missing.Set(k, v)
		}
	}
	if len(missing) == 0 {
		return
	}

	if u.RawQuery == "" {
		u.RawQuery = missing.Encode()
		return
	}
	u.RawQuery += "&" + missing.Encode()
}

// applyRouteParams exposes the route parameters through Request.PathValue.
func applyRouteParams(r *http.Request, rpcHttp *pb.RpcHttp) {
	forEachHTTPValue(rpcHttp.GetParams(), rpcHttp.GetNullableParams(), r.SetPathValue)
}
