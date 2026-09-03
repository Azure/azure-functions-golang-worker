package durabletask

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/api"
)

// checkStatusRetryAfterSeconds is the polling interval suggested to callers of
// a starter endpoint. It matches the value the other language workers send.
const checkStatusRetryAfterSeconds = "10"

// HTTPManagementURLs is the standard Durable Functions "check status" payload:
// the new instance's ID plus the host's management webhook URLs for it.
//
// SendEventPostURI contains an {eventName} placeholder and the terminate,
// rewind, suspend, and resume URLs contain a {text} placeholder for the reason.
// Callers substitute those before use, which mirrors the payload the other
// language workers return.
//
// RewindPostURI is included for parity with the in-process extension, the .NET
// isolated worker, and the JavaScript worker, all of which emit it. Rewind is
// a preview feature implemented only by the Azure Storage backend; on other
// backends the URL is well-formed but the operation is not supported.
type HTTPManagementURLs struct {
	ID                    string `json:"id"`
	StatusQueryGetURI     string `json:"statusQueryGetUri"`
	SendEventPostURI      string `json:"sendEventPostUri"`
	TerminatePostURI      string `json:"terminatePostUri"`
	RewindPostURI         string `json:"rewindPostUri"`
	PurgeHistoryDeleteURI string `json:"purgeHistoryDeleteUri"`
	RestartPostURI        string `json:"restartPostUri"`
	SuspendPostURI        string `json:"suspendPostUri"`
	ResumePostURI         string `json:"resumePostUri"`
}

// ManagementURLs returns the host's management webhook URLs for instanceID.
//
// The base URL comes from the durable client binding when the host supplied one
// (it already points at the durable webhook root). Otherwise it is derived from
// r, which is the case for clients created from [EnvGrpcEndpoint].
func (c *Client) ManagementURLs(r *http.Request, instanceID string) HTTPManagementURLs {
	instanceURL := c.instanceURL(r, instanceID)
	reason := "reason={text}"

	return HTTPManagementURLs{
		ID:                    instanceID,
		StatusQueryGetURI:     withQuery(instanceURL, c.queryParams),
		SendEventPostURI:      withQuery(instanceURL+"/raiseEvent/{eventName}", c.queryParams),
		TerminatePostURI:      withQuery(instanceURL+"/terminate", reason, c.queryParams),
		RewindPostURI:         withQuery(instanceURL+"/rewind", reason, c.queryParams),
		PurgeHistoryDeleteURI: withQuery(instanceURL, c.queryParams),
		RestartPostURI:        withQuery(instanceURL+"/restart", c.queryParams),
		SuspendPostURI:        withQuery(instanceURL+"/suspend", reason, c.queryParams),
		ResumePostURI:         withQuery(instanceURL+"/resume", reason, c.queryParams),
	}
}

// WriteCheckStatusResponse writes the standard Durable Functions starter
// response: HTTP 202 with the management URLs for instanceID as the body, a
// Location header pointing at the status endpoint, and a Retry-After hint.
//
// This is the canonical reply from a function that starts an orchestration:
//
//	func StartHelloCities(w http.ResponseWriter, r *http.Request) {
//	    client, ok := durabletask.ClientFromContext(r.Context())
//	    if !ok {
//	        http.Error(w, "durable client unavailable", http.StatusInternalServerError)
//	        return
//	    }
//	    id, err := client.ScheduleNewOrchestration(r.Context(), "HelloCities", nil)
//	    if err != nil {
//	        http.Error(w, err.Error(), http.StatusInternalServerError)
//	        return
//	    }
//	    _ = client.WriteCheckStatusResponse(w, r, id)
//	}
func (c *Client) WriteCheckStatusResponse(w http.ResponseWriter, r *http.Request, instanceID string) error {
	// Serialize before touching the response. Writing the header first would
	// mean an encoding failure produced a 202 with an empty body, which reads
	// as success to every client that does not parse the payload.
	body, err := json.Marshal(c.ManagementURLs(r, instanceID))
	if err != nil {
		return fmt.Errorf("write check status response: %w", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Location", c.ManagementURLs(r, instanceID).StatusQueryGetURI)
	w.Header().Set("Retry-After", checkStatusRetryAfterSeconds)
	w.WriteHeader(http.StatusAccepted)
	_, err = w.Write(body)
	return err
}

// StatusResponse is the standard Durable Functions status payload: the shape
// the host's own management API returns from
// /runtime/webhooks/durabletask/instances/{id}, and the shape every Durable
// Functions client and tool expects.
//
// Input, CustomStatus and Output are the orchestration's serialized values
// embedded as-is, so a JSON object stays an object rather than becoming a
// quoted string. They are null when the orchestration carried no such value.
type StatusResponse struct {
	Name            string          `json:"name,omitempty"`
	InstanceID      string          `json:"instanceId"`
	RuntimeStatus   string          `json:"runtimeStatus"`
	Input           json.RawMessage `json:"input"`
	CustomStatus    json.RawMessage `json:"customStatus"`
	Output          json.RawMessage `json:"output"`
	CreatedTime     time.Time       `json:"createdTime"`
	LastUpdatedTime time.Time       `json:"lastUpdatedTime"`
}

// NewStatusResponse renders orchestration metadata as the standard Durable
// Functions status payload.
//
// Use this when an app exposes its own status endpoint. Apps that are happy to
// send callers to the host's endpoint can skip it: the statusQueryGetUri in
// the check-status reply already points there.
func NewStatusResponse(m *api.OrchestrationMetadata) StatusResponse {
	if m == nil {
		return StatusResponse{}
	}
	return StatusResponse{
		Name:            m.Name,
		InstanceID:      string(m.InstanceID),
		RuntimeStatus:   RuntimeStatus(m),
		Input:           rawJSON(m.SerializedInput),
		CustomStatus:    rawJSON(m.SerializedCustomStatus),
		Output:          rawJSON(m.SerializedOutput),
		CreatedTime:     m.CreatedAt,
		LastUpdatedTime: m.LastUpdatedAt,
	}
}

// WriteStatusResponse writes the standard Durable Functions status payload for
// m, with HTTP 200.
//
// This is the counterpart to [Client.WriteCheckStatusResponse]: that one
// answers a starter, this one answers a status query. Both exist so an app's
// endpoints speak the same wire contract as the host's, which durabletask-go
// has no notion of.
//
//	meta, err := client.FetchOrchestrationMetadata(r.Context(), api.InstanceID(id))
//	if errors.Is(err, api.ErrInstanceNotFound) {
//	    http.Error(w, "no such instance", http.StatusNotFound)
//	    return
//	}
//	_ = durabletask.WriteStatusResponse(w, meta)
//
// It is a package-level function rather than a method because, unlike the
// management URLs, the status payload needs nothing the host supplied through
// the durable client binding.
func WriteStatusResponse(w http.ResponseWriter, m *api.OrchestrationMetadata) error {
	// Serialize first, for the same reason as WriteCheckStatusResponse: an
	// encoding failure after the header is written yields a 200 with an empty
	// body. That is exactly what a non-JSON custom status used to produce
	// before rawJSON started quoting it.
	body, err := json.Marshal(NewStatusResponse(m))
	if err != nil {
		return fmt.Errorf("write status response: %w", err)
	}

	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(body)
	return err
}

// rawJSON embeds an already-serialized value, or null when there is none.
//
// Most values arrive as JSON: inputs and outputs are marshaled by
// durabletask-go before they reach the history. Custom status does not —
// [task.OrchestrationContext.SetCustomStatus] takes a plain string and stores
// it verbatim, so "awaiting approval" would be invalid JSON if embedded as-is
// and would abort encoding of the whole payload. Anything that is not valid
// JSON is therefore encoded as a JSON string, which is also how the host
// renders it in its own status payload.
func rawJSON(serialized string) json.RawMessage {
	if serialized == "" {
		return json.RawMessage("null")
	}
	if json.Valid([]byte(serialized)) {
		return json.RawMessage(serialized)
	}
	quoted, err := json.Marshal(serialized)
	if err != nil {
		return json.RawMessage("null")
	}
	return json.RawMessage(quoted)
}

// instanceURL builds the management URL for a single instance.
//
// The origin is chosen the way the .NET isolated worker chooses it, because
// that worker faces the same problem this one does: the request it sees has
// been forwarded by the Functions host and its Host header names the worker's
// own loopback listener, not the caller. Both therefore prefer the forwarded
// headers, which the host populates with the caller-visible host and scheme,
// and which are the only signal that reflects a custom domain or an API
// gateway in front of the app.
//
// Where this worker differs from .NET isolated is the fallback. That worker
// falls back to the request URL, which for us would be http://127.0.0.1:PORT.
// The durable client binding supplies a usable base URL instead, so it is
// preferred over anything derived from the connection. The path always comes
// from the binding when one is present, since the webhook root is
// configurable and only the host knows it.
func (c *Client) instanceURL(r *http.Request, instanceID string) string {
	escaped := url.PathEscape(instanceID)
	base := strings.TrimSuffix(c.httpBaseURL, "/")

	if origin := forwardedOrigin(r); origin != "" {
		path := durableWebhookPath
		if base != "" {
			if u, err := url.Parse(base); err == nil && u.Path != "" {
				path = u.Path
			}
		}
		return origin + path + "/instances/" + escaped
	}

	if base != "" {
		return base + "/instances/" + escaped
	}
	return requestOrigin(r) + durableWebhookPath + "/instances/" + escaped
}

// durableWebhookPath is where the host serves the durable management webhooks
// unless it tells us otherwise through the binding's httpBaseUrl.
const durableWebhookPath = "/runtime/webhooks/durabletask"

// forwardedOrigin reconstructs the caller-visible origin from the proxy
// headers the Functions front end and the host's request forwarder set.
// Returns "" when the host did not forward one, in which case the caller
// falls back to the binding-supplied base URL.
//
// X-Forwarded-Host is required; X-Forwarded-Proto is advisory and defaults to
// https, which is what the front end terminates for every non-local request.
func forwardedOrigin(r *http.Request) string {
	if r == nil {
		return ""
	}
	host := r.Header.Get("X-Forwarded-Host")
	if host == "" {
		return ""
	}
	scheme := r.Header.Get("X-Forwarded-Proto")
	if scheme == "" {
		scheme = "https"
	}
	return scheme + "://" + host
}

// requestOrigin reconstructs an origin from the connection itself. This is a
// last resort: on the host's HTTP-streaming path r.Host is the worker's own
// listener, so this only produces something useful when a caller reaches the
// worker directly, as tests do.
func requestOrigin(r *http.Request) string {
	if r == nil {
		return ""
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

// withQuery appends the non-empty query fragments to base.
func withQuery(base string, fragments ...string) string {
	present := make([]string, 0, len(fragments))
	for _, fragment := range fragments {
		if fragment != "" {
			present = append(present, fragment)
		}
	}
	if len(present) == 0 {
		return base
	}
	return base + "?" + strings.Join(present, "&")
}
