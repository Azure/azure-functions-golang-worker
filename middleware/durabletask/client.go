package durabletask

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	dtclient "github.com/microsoft/durabletask-go/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// EnvGrpcEndpoint is the environment variable the default client dials when
// the middleware is constructed without an explicit [WithClient]. It carries
// the address of the Durable Task gRPC endpoint the host (or the Durable Task
// Scheduler) exposes for management operations, e.g. "127.0.0.1:4001".
const EnvGrpcEndpoint = "DURABLE_TASK_GRPC_ENDPOINT"

// ErrInstanceNotFound is returned by [Client.GetStatus] when no orchestration
// instance with the given ID exists.
var ErrInstanceNotFound = errors.New("durabletask: orchestration instance not found")

// Client starts and manages orchestration instances. It is a thin wrapper
// around durabletask-go's client.TaskHubGrpcClient: every management call
// delegates to the upstream client, which speaks the Durable Task gRPC
// protocol (TaskHubSidecarService). The endpoint is whatever exposes that
// protocol — the Durable Task Scheduler (DTS), a durabletask-go sidecar, or
// the Functions host's durable gRPC endpoint delivered via the DurableClient
// binding.
//
// This is the management half of the integration. The execution half
// (orchestrator replay) is handled separately by the middleware against the
// Functions trigger payload — see [Durable.Wrap]. Both share the same
// durabletask-go programming model (task.OrchestrationContext), so the same
// orchestrator function is driven by either path.
type Client struct {
	inner *dtclient.TaskHubGrpcClient
	conn  *grpc.ClientConn // owned (non-nil) only when created via Dial

	// httpBaseURL is the durable webhook root the host advertised through the
	// durable client binding (…/runtime/webhooks/durabletask). Empty when the
	// client was not created from a binding, in which case the management URLs
	// are derived from the incoming request instead.
	httpBaseURL string
	// queryParams carries the query string the host requires on management
	// calls, typically the auth code and task hub.
	queryParams string
}

// NewClient wraps an existing gRPC connection. Use this when you manage the
// connection yourself (or in tests, with an in-memory listener). The caller
// owns the connection's lifecycle.
func NewClient(conn grpc.ClientConnInterface) *Client {
	return &Client{inner: dtclient.NewTaskHubGrpcClient(conn, backend.DefaultLogger())}
}

// Dial connects to a Durable Task gRPC endpoint and returns a [Client] that
// owns the connection (close it with [Client.Close]). It uses insecure
// transport, suitable for a local sidecar / DTS emulator; supply your own
// connection via [NewClient] for TLS.
func Dial(endpoint string) (*Client, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("durabletask: dial %q: %w", endpoint, err)
	}
	return &Client{
		inner: dtclient.NewTaskHubGrpcClient(conn, backend.DefaultLogger()),
		conn:  conn,
	}, nil
}

// grpcTarget converts a durable client binding's rpcBaseUrl (an HTTP(S) URL
// like "http://127.0.0.1:54321/", as the Functions host delivers it) into a
// gRPC dial target ("127.0.0.1:54321"). A value that is already a bare
// host:port is returned unchanged.
func grpcTarget(rpcBaseURL string) string {
	if u, err := url.Parse(rpcBaseURL); err == nil && u.Host != "" {
		return u.Host
	}
	return rpcBaseURL
}

// Close releases the underlying connection if this Client created it.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// ScheduleNewOrchestration starts a new instance of the named orchestrator
// with the given (JSON-serializable) input and returns the new instance ID.
func (c *Client) ScheduleNewOrchestration(ctx context.Context, name string, input any) (string, error) {
	var opts []api.NewOrchestrationOptions
	if input != nil {
		opts = append(opts, api.WithInput(input))
	}
	id, err := c.inner.ScheduleNewOrchestration(ctx, name, opts...)
	if err != nil {
		return "", err
	}
	return string(id), nil
}

// RaiseEvent delivers an external event to a waiting orchestration instance.
// This is the channel used to deliver human-in-the-loop responses to an
// orchestration blocked in WaitForSingleEvent.
func (c *Client) RaiseEvent(ctx context.Context, instanceID, eventName string, payload any) error {
	var opts []api.RaiseEventOptions
	if payload != nil {
		opts = append(opts, api.WithEventPayload(payload))
	}
	return c.inner.RaiseEvent(ctx, api.InstanceID(instanceID), eventName, opts...)
}

// Terminate terminates a running orchestration instance.
func (c *Client) Terminate(ctx context.Context, instanceID, reason string) error {
	return c.inner.TerminateOrchestration(ctx, api.InstanceID(instanceID), api.WithOutput(reason))
}

// Suspend suspends a running orchestration instance.
func (c *Client) Suspend(ctx context.Context, instanceID, reason string) error {
	return c.inner.SuspendOrchestration(ctx, api.InstanceID(instanceID), reason)
}

// Resume resumes a suspended orchestration instance.
func (c *Client) Resume(ctx context.Context, instanceID, reason string) error {
	return c.inner.ResumeOrchestration(ctx, api.InstanceID(instanceID), reason)
}

// Purge deletes the state and history of an orchestration instance.
func (c *Client) Purge(ctx context.Context, instanceID string) error {
	return c.inner.PurgeOrchestrationState(ctx, api.InstanceID(instanceID))
}

// OrchestrationStatus is the status of an orchestration instance.
// RuntimeStatus is one of: Pending, Running, Completed, ContinuedAsNew,
// Failed, Terminated, Suspended. Input, Output, and CustomStatus are the
// serialized (JSON) values; the orchestrator's ctx.SetCustomStatus surfaces
// in CustomStatus, making it the progress channel.
type OrchestrationStatus struct {
	InstanceID    string    `json:"instanceId"`
	Name          string    `json:"name,omitempty"`
	RuntimeStatus string    `json:"runtimeStatus"`
	Input         string    `json:"input,omitempty"`
	Output        string    `json:"output,omitempty"`
	CustomStatus  string    `json:"customStatus,omitempty"`
	CreatedAt     time.Time `json:"createdAt,omitempty"`
	LastUpdatedAt time.Time `json:"lastUpdatedAt,omitempty"`
}

// GetStatus queries the runtime status of an orchestration instance. Returns
// [ErrInstanceNotFound] when the instance does not exist.
func (c *Client) GetStatus(ctx context.Context, instanceID string) (*OrchestrationStatus, error) {
	meta, err := c.inner.FetchOrchestrationMetadata(ctx, api.InstanceID(instanceID))
	if err != nil {
		if errors.Is(err, api.ErrInstanceNotFound) {
			return nil, ErrInstanceNotFound
		}
		return nil, err
	}
	return toStatus(meta), nil
}

// WaitForCompletion blocks until the orchestration reaches a terminal state
// (or ctx is cancelled) and returns its final status.
func (c *Client) WaitForCompletion(ctx context.Context, instanceID string) (*OrchestrationStatus, error) {
	meta, err := c.inner.WaitForOrchestrationCompletion(ctx, api.InstanceID(instanceID))
	if err != nil {
		return nil, err
	}
	return toStatus(meta), nil
}

func toStatus(m *api.OrchestrationMetadata) *OrchestrationStatus {
	return &OrchestrationStatus{
		InstanceID:    string(m.InstanceID),
		Name:          m.Name,
		RuntimeStatus: runtimeStatusString(m.RuntimeStatus.String()),
		Input:         m.SerializedInput,
		Output:        m.SerializedOutput,
		CustomStatus:  m.SerializedCustomStatus,
		CreatedAt:     m.CreatedAt,
		LastUpdatedAt: m.LastUpdatedAt,
	}
}

// runtimeStatusString trims the protobuf enum prefix and normalizes casing,
// turning "ORCHESTRATION_STATUS_RUNNING" into "Running".
func runtimeStatusString(enum string) string {
	s := strings.TrimPrefix(enum, "ORCHESTRATION_STATUS_")
	if s == "" {
		return enum
	}
	parts := strings.Split(strings.ToLower(s), "_")
	for i, p := range parts {
		if p != "" {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, "")
}

// defaultClientFromEnv builds a [Client] from EnvGrpcEndpoint, or returns nil
// when the variable is unset (so the middleware can run without a client when
// only orchestrator/activity execution is needed).
func defaultClientFromEnv() *Client {
	endpoint := os.Getenv(EnvGrpcEndpoint)
	if endpoint == "" {
		return nil
	}
	c, err := Dial(endpoint)
	if err != nil {
		return nil
	}
	return c
}

// clientContextKey is the unexported context key under which the durable
// [Client] is stashed for non-orchestration invocations.
type clientContextKey struct{}

// contextWithClient returns a context carrying the durable client.
func contextWithClient(parent context.Context, c *Client) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithValue(parent, clientContextKey{}, c)
}

// ClientFromContext returns the durable [Client] the middleware attached to
// the invocation context. HTTP starter functions call it via r.Context():
//
//	func StartHelloCities(w http.ResponseWriter, r *http.Request) {
//	    client, ok := durabletask.ClientFromContext(r.Context())
//	    if !ok { http.Error(w, "durable client unavailable", 500); return }
//	    id, _ := client.ScheduleNewOrchestration(r.Context(), "HelloCities", nil)
//	    ...
//	}
//
// The boolean is false when the durable middleware has no client configured
// (no [WithClient] and EnvGrpcEndpoint unset).
func ClientFromContext(ctx context.Context) (*Client, bool) {
	if ctx == nil {
		return nil, false
	}
	c, ok := ctx.Value(clientContextKey{}).(*Client)
	return c, ok && c != nil
}
