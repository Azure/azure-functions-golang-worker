# HTTP requests and responses

How the Go worker maps HTTP requests and responses between the Azure Functions
host and a standard `net/http` handler.

HTTP-triggered functions are plain `func(http.ResponseWriter, *http.Request)`
handlers. The worker rebuilds the request the host received, and turns what the
handler writes back into the response the host sends to the client. The same
handler code behaves the same way on either transport described in
[Triggers and bindings](triggers-and-bindings.md).

## Request headers

Headers sent by the client are available on `r.Header`. Use `r.Header.Get` so
the lookup is case insensitive.

```go
func handler(w http.ResponseWriter, r *http.Request) {
	contentType := r.Header.Get("Content-Type")
	requestID := r.Header.Get("X-Request-Id")
	// ...
}
```

The host sends a header that appeared more than once as a single value joined
with commas, so `r.Header.Values` returns one entry.

A header sent with no value is kept as an empty string. A header the host marks
as null is skipped.

## Query string and route parameters

The query string is read with `r.URL.Query()`.

Route parameters are read with `r.PathValue`, the same way as with the Go 1.22
`ServeMux`. For a function registered with `sdk.WithRoute("orders/{id}")`:

```go
app.HTTP("getOrder", func(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// ...
}, sdk.WithRoute("orders/{id}"))
```

Route matching is done by the host, so the worker does not parse the route
template. A parameter that is absent returns an empty string.

## Request origin

When the host forwards a request to the worker over the loopback listener, the
connection comes from the host rather than the client. The worker restores the
client-visible origin from the `X-Forwarded-Host` and `X-Forwarded-Proto`
headers that the host adds.

| Forwarded headers | `r.Host` | `r.URL.Scheme` and `r.URL.Host` |
| --- | --- | --- |
| Valid host and `http` or `https` proto | Forwarded host | Set |
| Valid host only | Forwarded host | Unchanged |
| Proto only, or no headers | Unchanged | Unchanged |
| Invalid host | Unchanged | Unchanged |

A host is valid when it is a plain host or `host:port`. A value containing a
path, query, credentials, or whitespace is ignored. When a header holds a
comma-separated list, the first entry is used. The `X-Forwarded-*` headers are
left in place so code that reads them directly keeps working.

This lets a handler build absolute URLs, such as a redirect `Location`, that a
client can reach.

## Response headers

Headers set on `w.Header()` are sent to the client. A header added more than
once is sent as one value joined with `, `, as described in RFC 9110. Header
names are case insensitive, so `x-trace` and `X-Trace` are treated as the same
header and their values are combined.

```go
w.Header().Add("Link", `</style.css>; rel="preload"`)
w.Header().Add("Link", `</app.js>; rel="preload"`)
```

## Cookies

Use `http.SetCookie` as in any Go server. Each cookie is sent to the host as a
structured cookie, so several cookies in one response are all delivered.

```go
http.SetCookie(w, &http.Cookie{
	Name:     "session",
	Value:    "abc123",
	Path:     "/",
	HttpOnly: true,
	Secure:   true,
	SameSite: http.SameSiteLaxMode,
})
http.SetCookie(w, &http.Cookie{Name: "theme", Value: "dark", MaxAge: 3600})
```

`Set-Cookie` is not sent as a joined header, because cookie values can contain
commas that would make them impossible to split again.

These attributes are forwarded.

- `Domain`
- `Path`
- `Expires`
- `MaxAge`, where `MaxAge: -1` sends `Max-Age=0` to delete the cookie
- `Secure`
- `HttpOnly`
- `SameSite`

An attribute the handler did not set stays unset, so the host applies its own
defaults. The one exception is `Path`. When it is unset the worker sends an
empty path, so the cookie keeps Go's default scope, the directory of the
request, and is not widened to `/`. The host cookie model has no field for
`Partitioned` or for unknown attributes, so those are not forwarded.

### Cookie values on the buffered gRPC transport

The host escapes the value of every structured cookie, the same way it does for
the .NET, Node.js and Python workers. Only letters, digits and `-`, `_`, `.`,
`~` are sent as written. A value such as `abc=` reaches the client as `abc%3D`.
The default transport sends `Set-Cookie` as written and does not do this.

To get the same cookie on both transports, use a value made only of those
characters, such as `base64.RawURLEncoding`, or decode it on the way in.

```go
http.SetCookie(w, &http.Cookie{Name: "token", Value: base64.RawURLEncoding.EncodeToString(raw)})

if c, err := r.Cookie("token"); err == nil {
	value, _ := url.QueryUnescape(c.Value)
	_ = value
}
```

`r.Cookie` returns the value as the client sent it, so a value the host escaped
needs `url.QueryUnescape` before use. The .NET isolated worker behaves the same
way and does not decode request cookies for you.

## Transports

The default transport proxies the request and response over the loopback
listener, so the body is streamed. Headers and cookies are handled by the
listener directly. Route parameters and the forwarded origin are applied by the
worker before the handler runs.

The buffered gRPC transport is used when `FUNCTIONS_GO_DISABLE_HTTP_PROXY` is
set, when the host does not support HTTP proxying, or when the listener cannot
start. In that case the worker rebuilds the request from the gRPC payload and
encodes the response as described above. The absolute request URL is sent by
the host, so no forwarded headers are needed.
