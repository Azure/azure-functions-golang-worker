package worker

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
	"github.com/azure/azure-functions-golang-worker/worker/proto/shared"
)

func nullableStr(s string) *shared.NullableString {
	return &shared.NullableString{String_: &shared.NullableString_Value{Value: s}}
}

// The host sends the nullable maps, with lowercase header keys, and leaves
// the plain maps empty.
func TestRpcHttpToRequest_NullableHeaders(t *testing.T) {
	req, err := RpcHttpToRequest(&pb.RpcHttp{
		Method: "POST",
		Url:    "http://example.com/api/echo",
		NullableHeaders: map[string]*shared.NullableString{
			"content-type":    nullableStr("application/json"),
			"x-request-id":    nullableStr("abc-123"),
			"accept-encoding": nullableStr("gzip, deflate"),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for header, want := range map[string]string{
		"Content-Type":    "application/json",
		"X-Request-Id":    "abc-123",
		"Accept-Encoding": "gzip, deflate",
	} {
		if got := req.Header.Get(header); got != want {
			t.Errorf("header %q = %q, want %q", header, got, want)
		}
	}
}

func TestRpcHttpToRequest_NullableHeaders_NullAndEmpty(t *testing.T) {
	req, err := RpcHttpToRequest(&pb.RpcHttp{
		Method: "GET",
		Url:    "http://example.com/",
		NullableHeaders: map[string]*shared.NullableString{
			"x-null":      nil,
			"x-null-body": {},
			"x-empty":     nullableStr(""),
			"x-set":       nullableStr("v"),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := req.Header["X-Null"]; ok {
		t.Error("nil nullable header should be skipped")
	}
	if _, ok := req.Header["X-Null-Body"]; ok {
		t.Error("nullable header with no value should be skipped")
	}
	if v, ok := req.Header["X-Empty"]; !ok || len(v) != 1 || v[0] != "" {
		t.Errorf("explicitly empty header should be kept, got %q (present=%v)", v, ok)
	}
	if got := req.Header.Get("X-Set"); got != "v" {
		t.Errorf("X-Set = %q, want %q", got, "v")
	}
}

func TestRpcHttpToRequest_NullableHeadersWinOverPlain(t *testing.T) {
	req, err := RpcHttpToRequest(&pb.RpcHttp{
		Method:  "GET",
		Url:     "http://example.com/",
		Headers: map[string]string{"X-Both": "plain", "X-Plain-Only": "p"},
		NullableHeaders: map[string]*shared.NullableString{
			"x-both": nullableStr("nullable"),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := req.Header.Get("X-Both"); got != "nullable" {
		t.Errorf("X-Both = %q, want the nullable value", got)
	}
	if got := req.Header.Get("X-Plain-Only"); got != "p" {
		t.Errorf("X-Plain-Only = %q, want %q", got, "p")
	}
	if n := len(req.Header.Values("X-Both")); n != 1 {
		t.Errorf("X-Both has %d values, want 1", n)
	}
}

// The URL is the source of truth for the query. Entries it already carries
// must not be added a second time.
func TestRpcHttpToRequest_QueryNotDuplicated(t *testing.T) {
	req, err := RpcHttpToRequest(&pb.RpcHttp{
		Method: "GET",
		Url:    "http://example.com/api/search?q=go%20lang&page=1",
		NullableQuery: map[string]*shared.NullableString{
			"q":    nullableStr("go lang"),
			"page": nullableStr("1"),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got, want := req.URL.RawQuery, "q=go%20lang&page=1"; got != want {
		t.Errorf("RawQuery = %q, want %q", got, want)
	}
	for _, key := range []string{"q", "page"} {
		if n := len(req.URL.Query()[key]); n != 1 {
			t.Errorf("query %q has %d values, want 1", key, n)
		}
	}
}

func TestRpcHttpToRequest_QueryAddedWhenMissingFromURL(t *testing.T) {
	req, err := RpcHttpToRequest(&pb.RpcHttp{
		Method: "GET",
		Url:    "http://example.com/api/search?a=1",
		NullableQuery: map[string]*shared.NullableString{
			"a":     nullableStr("1"),
			"b":     nullableStr("2"),
			"empty": nullableStr(""),
			"null":  nil,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	q := req.URL.Query()
	if got := q["a"]; !reflect.DeepEqual(got, []string{"1"}) {
		t.Errorf("a = %q, want [1]", got)
	}
	if got := q.Get("b"); got != "2" {
		t.Errorf("b = %q, want 2", got)
	}
	if !q.Has("empty") {
		t.Error("explicitly empty query entry should be kept")
	}
	if q.Has("null") {
		t.Error("null query entry should be skipped")
	}
}

func TestRpcHttpToRequest_RouteParams(t *testing.T) {
	req, err := RpcHttpToRequest(&pb.RpcHttp{
		Method: "GET",
		Url:    "http://example.com/api/orders/42",
		Params: map[string]string{"plain": "p", "both": "plain"},
		NullableParams: map[string]*shared.NullableString{
			"id":   nullableStr("42"),
			"both": nullableStr("nullable"),
			"null": nil,
			"zero": nullableStr(""),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for name, want := range map[string]string{
		"id":    "42",
		"plain": "p",
		"both":  "nullable",
		"zero":  "",
		"null":  "",
	} {
		if got := req.PathValue(name); got != want {
			t.Errorf("PathValue(%q) = %q, want %q", name, got, want)
		}
	}
}

// Route params must survive the context copies that middleware and
// http.Handler wrappers make with Request.WithContext.
func TestApplyRouteParams_SurvivesWithContext(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/api/orders/7", nil)
	applyRouteParams(r, &pb.RpcHttp{
		NullableParams: map[string]*shared.NullableString{"id": nullableStr("7")},
	})

	copied := r.WithContext(r.Context())
	if got := copied.PathValue("id"); got != "7" {
		t.Errorf("PathValue after WithContext = %q, want 7", got)
	}
}

func TestEncodeResponseHeaders_JoinsRepeatedValues(t *testing.T) {
	h := http.Header{}
	h.Add("Link", `</a>; rel="preload"`)
	h.Add("Link", `</b>; rel="preload"`)
	h.Set("Content-Type", "text/plain")
	h.Set("X-Empty", "")
	h["Empty"] = nil

	got := encodeResponseHeaders(h)

	if v, ok := got["X-Empty"]; !ok || v != "" {
		t.Errorf("X-Empty = %q, present=%v, want present and empty", v, ok)
	}

	if want := `</a>; rel="preload", </b>; rel="preload"`; got["Link"] != want {
		t.Errorf("Link = %q, want %q", got["Link"], want)
	}
	if got["Content-Type"] != "text/plain" {
		t.Errorf("Content-Type = %q, want text/plain", got["Content-Type"])
	}
	if _, ok := got["Empty"]; ok {
		t.Error("header with no values should be omitted")
	}
}

func TestEncodeHTTPResponse_SetCookieBecomesCookies(t *testing.T) {
	proxy := NewResponseWriterProxy()
	http.SetCookie(proxy, &http.Cookie{Name: "a", Value: "1", Path: "/"})
	http.SetCookie(proxy, &http.Cookie{Name: "b", Value: "2", Path: "/x"})
	proxy.Header().Add("Link", "</one>")
	proxy.Header().Add("Link", "</two>")
	proxy.WriteHeader(http.StatusOK)

	httpData := encodeHTTPResponse(proxy).GetHttp()
	if httpData == nil {
		t.Fatal("expected HTTP data")
	}

	if _, ok := httpData.Headers["Set-Cookie"]; ok {
		t.Errorf("Set-Cookie must not be sent as a joined header: %q", httpData.Headers["Set-Cookie"])
	}
	if got, want := httpData.Headers["Link"], "</one>, </two>"; got != want {
		t.Errorf("Link = %q, want %q", got, want)
	}

	if len(httpData.Cookies) != 2 {
		t.Fatalf("got %d cookies, want 2", len(httpData.Cookies))
	}
	for i, want := range []struct{ name, value, path string }{
		{"a", "1", "/"},
		{"b", "2", "/x"},
	} {
		c := httpData.Cookies[i]
		if c.Name != want.name || c.Value != want.value {
			t.Errorf("cookie[%d] = %s=%s, want %s=%s", i, c.Name, c.Value, want.name, want.value)
		}
		if got := c.GetPath().GetValue(); got != want.path {
			t.Errorf("cookie[%d] path = %q, want %q", i, got, want.path)
		}
	}
}

func TestEncodeHTTPResponse_NonCanonicalHeaderKeys(t *testing.T) {
	proxy := NewResponseWriterProxy()
	h := proxy.Header()
	h["set-cookie"] = []string{"a=1; Path=/", "b=2; Path=/x"}
	h["SET-COOKIE"] = []string{"c=3"}
	h["x-trace"] = []string{"one"}
	h["X-Trace"] = []string{"two"}
	h["content-type"] = []string{"text/plain"}
	proxy.WriteHeader(http.StatusOK)

	httpData := encodeHTTPResponse(proxy).GetHttp()

	for k := range httpData.Headers {
		if k != http.CanonicalHeaderKey(k) {
			t.Errorf("header key %q is not canonical", k)
		}
	}
	if _, ok := httpData.Headers["Set-Cookie"]; ok {
		t.Errorf("Set-Cookie must not be sent as a joined header: %q", httpData.Headers["Set-Cookie"])
	}
	if got, want := httpData.Headers["X-Trace"], "two, one"; got != want {
		t.Errorf("X-Trace = %q, want %q", got, want)
	}
	if got := httpData.Headers["Content-Type"]; got != "text/plain" {
		t.Errorf("Content-Type = %q, want text/plain", got)
	}

	var names []string
	for _, c := range httpData.Cookies {
		names = append(names, c.Name)
	}
	if want := []string{"c", "a", "b"}; !reflect.DeepEqual(names, want) {
		t.Errorf("cookies = %v, want %v", names, want)
	}
}

func TestEncodeCookies_Attributes(t *testing.T) {
	expires := time.Date(2030, time.January, 2, 3, 4, 5, 0, time.UTC)
	cookies := []*http.Cookie{
		{
			Name:     "full",
			Value:    "v",
			Domain:   "example.com",
			Path:     "/app",
			Expires:  expires,
			MaxAge:   60,
			Secure:   true,
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
		},
	}

	out := encodeCookies(cookies)
	if len(out) != 1 {
		t.Fatalf("got %d cookies, want 1", len(out))
	}
	c := out[0]

	if got := c.GetDomain().GetValue(); got != "example.com" {
		t.Errorf("domain = %q", got)
	}
	if got := c.GetPath().GetValue(); got != "/app" {
		t.Errorf("path = %q", got)
	}
	if got := c.GetExpires().GetValue().AsTime(); !got.Equal(expires) {
		t.Errorf("expires = %v, want %v", got, expires)
	}
	if got := c.GetMaxAge().GetValue(); got != 60 {
		t.Errorf("max-age = %v, want 60", got)
	}
	if !c.GetSecure().GetValue() {
		t.Error("secure should be true")
	}
	if !c.GetHttpOnly().GetValue() {
		t.Error("httponly should be true")
	}
	if c.SameSite != pb.RpcHttpCookie_Strict {
		t.Errorf("samesite = %v, want Strict", c.SameSite)
	}
}

// Attributes the handler never set stay unset, so the host applies its own
// defaults instead of receiving false or zero values. Path is the exception.
// It is sent as an explicit empty string so the host does not default it to
// "/", which would widen a cookie that net/http scopes to the request's
// directory.
func TestEncodeCookies_UnsetAttributesStayNull(t *testing.T) {
	out := encodeCookies([]*http.Cookie{{Name: "n", Value: "v"}})
	c := out[0]

	if c.Domain != nil || c.Expires != nil ||
		c.MaxAge != nil || c.Secure != nil || c.HttpOnly != nil {
		t.Errorf("unset attributes should be nil, got %+v", c)
	}
	if c.Path == nil || c.GetPath().GetValue() != "" {
		t.Errorf("unset path should be an explicit empty string, got %v", c.Path)
	}
	if c.SameSite != pb.RpcHttpCookie_None {
		t.Errorf("samesite = %v, want None", c.SameSite)
	}
}

func TestEncodeCookies_SameSite(t *testing.T) {
	tests := []struct {
		mode http.SameSite
		want pb.RpcHttpCookie_SameSite
	}{
		{http.SameSiteDefaultMode, pb.RpcHttpCookie_None},
		{http.SameSiteLaxMode, pb.RpcHttpCookie_Lax},
		{http.SameSiteStrictMode, pb.RpcHttpCookie_Strict},
		{http.SameSiteNoneMode, pb.RpcHttpCookie_ExplicitNone},
	}
	for _, tc := range tests {
		out := encodeCookies([]*http.Cookie{{Name: "c", Value: "v", SameSite: tc.mode}})
		if out[0].SameSite != tc.want {
			t.Errorf("SameSite(%d) = %v, want %v", tc.mode, out[0].SameSite, tc.want)
		}
	}
}

// net/http reports an absent Max-Age as 0 and "Max-Age=0" as -1.
func TestEncodeCookies_MaxAge(t *testing.T) {
	tests := []struct {
		name   string
		maxAge int
		want   *float64
	}{
		{"unset", 0, nil},
		{"delete now", -1, ptr(0.0)},
		{"positive", 3600, ptr(3600.0)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := encodeCookies([]*http.Cookie{{Name: "c", Value: "v", MaxAge: tc.maxAge}})
			got := out[0].MaxAge
			if tc.want == nil {
				if got != nil {
					t.Fatalf("max-age = %v, want unset", got.GetValue())
				}
				return
			}
			if got == nil || got.GetValue() != *tc.want {
				t.Fatalf("max-age = %v, want %v", got, *tc.want)
			}
		})
	}
}

func TestEncodeCookies_None(t *testing.T) {
	if got := encodeCookies(nil); got != nil {
		t.Errorf("encodeCookies(nil) = %v, want nil", got)
	}
}

func ptr[T any](v T) *T { return &v }
