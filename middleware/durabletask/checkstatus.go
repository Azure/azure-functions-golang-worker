package durabletask

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// checkStatusRetryAfterSeconds is the polling interval suggested to callers of
// a starter endpoint. It matches the value the other language workers send.
const checkStatusRetryAfterSeconds = "10"

// HTTPManagementURLs is the standard Durable Functions "check status" payload:
// the new instance's ID plus the host's management webhook URLs for it.
//
// SendEventPostURI contains an {eventName} placeholder and the terminate,
// suspend, and resume URLs contain a {text} placeholder for the reason. Callers
// substitute those before use, which mirrors the payload the other language
// workers return.
type HTTPManagementURLs struct {
	ID                    string `json:"id"`
	StatusQueryGetURI     string `json:"statusQueryGetUri"`
	SendEventPostURI      string `json:"sendEventPostUri"`
	TerminatePostURI      string `json:"terminatePostUri"`
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
	urls := c.ManagementURLs(r, instanceID)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Location", urls.StatusQueryGetURI)
	w.Header().Set("Retry-After", checkStatusRetryAfterSeconds)
	w.WriteHeader(http.StatusAccepted)

	if err := json.NewEncoder(w).Encode(urls); err != nil {
		return fmt.Errorf("write check status response: %w", err)
	}
	return nil
}

// instanceURL builds the management URL for a single instance.
//
// The two bases differ in shape: the host-supplied base already includes the
// durable webhook path, while a base derived from the request is the bare
// origin and needs it appended.
func (c *Client) instanceURL(r *http.Request, instanceID string) string {
	escaped := url.PathEscape(instanceID)
	if c.httpBaseURL != "" {
		return strings.TrimSuffix(c.httpBaseURL, "/") + "/instances/" + escaped
	}
	return requestOrigin(r) + "/runtime/webhooks/durabletask/instances/" + escaped
}

// requestOrigin reconstructs the externally visible origin of r, preferring the
// proxy headers the Functions front end sets over the local connection details.
func requestOrigin(r *http.Request) string {
	if r == nil {
		return ""
	}

	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if forwarded := r.Header.Get("X-Forwarded-Proto"); forwarded != "" {
		scheme = forwarded
	}

	host := r.Host
	if forwarded := r.Header.Get("X-Forwarded-Host"); forwarded != "" {
		host = forwarded
	}
	return scheme + "://" + host
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
