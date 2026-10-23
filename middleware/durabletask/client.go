package durabletask

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/microsoft/durabletask-go/api"
	dtclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// EnvGrpcEndpoint is the environment variable the default client dials when
// the middleware is constructed without an explicit [WithClient]. It carries
// the address of the Durable Task gRPC endpoint the host (or the Durable Task
// Scheduler) exposes for management operations, e.g. "127.0.0.1:4001".
const EnvGrpcEndpoint = "DURABLE_TASK_GRPC_ENDPOINT"

// Client starts and manages orchestration instances.
//
// It embeds durabletask-go's [dtclient.TaskHubGrpcClient], so every operation
// that client offers is available directly and nothing needs to be wrapped
// here to be reachable:
//
//	id, err := client.ScheduleNewOrchestration(ctx, "ProcessExpense", api.WithInput(expense))
//	meta, err := client.FetchOrchestrationMetadata(ctx, id)
//	err = client.RaiseEvent(ctx, id, "ApprovalDecision", api.WithEventPayload(decision))
//
// The endpoint is whatever speaks the Durable Task gRPC protocol
// (TaskHubSidecarService): the Durable Task Scheduler, a durabletask-go
// sidecar, or the Functions host's durable gRPC endpoint delivered through the
// durableClient binding.
//
// Methods declared on Client add connection ownership, host webhook details,
// and reserved Functions payload validation. Management execution and payload
// storage remain upstream.
//
// This is the management half of the integration. The execution half
// (orchestrator replay) is handled by the adapter registered through
// [Durable.Orchestrator], inside the middleware chain. Both share the same
// durabletask-go programming model (task.OrchestrationContext), so the same
// orchestrator function is driven by either path.
type Client struct {
	*dtclient.TaskHubGrpcClient

	conn          *grpc.ClientConn // owned (non-nil) only when created via Dial
	connection    grpc.ClientConnInterface
	largePayloads *api.LargePayloadOptions

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
func NewClient(conn grpc.ClientConnInterface, opts ...ClientOption) *Client {
	c := &Client{connection: conn}
	for _, opt := range opts {
		opt(c)
	}
	c.configurePayloads(c.largePayloads)
	return c
}

// ClientOption configures a management client.
type ClientOption func(*Client)

// WithClientLargePayloads configures management payload offloading. Pass the
// same options to middleware WithLargePayloads when executing under Functions.
func WithClientLargePayloads(options *api.LargePayloadOptions) ClientOption {
	return func(c *Client) { c.largePayloads = mustNormalizeLargePayloads(options) }
}

func (c *Client) configurePayloads(options *api.LargePayloadOptions) {
	c.largePayloads = options
	converter := api.DefaultDataConverter()
	if options != nil {
		converter = functionsPayloadConverter{converter}
	}
	c.TaskHubGrpcClient = dtclient.NewTaskHubGrpcClient(functionsPayloadConnection{c.connection}, api.NewSlogLogger(slog.Default()),
		dtclient.WithLargePayloads(options),
		dtclient.WithDataConverter(converter))
}

// The final option validates the configured request before upstream offloading,
// including raw options which intentionally bypass the data converter.
func payloadValidationOption[Request proto.Message, Option ~func(Request, api.DataConverter) error](_ Option) Option {
	return func(request Request, _ api.DataConverter) error {
		return visitPayloadFields(request.ProtoReflect(), func(value string) (string, error) {
			return value, rejectPayloadEnvelope(value)
		})
	}
}

// ScheduleNewOrchestration validates reserved Functions payload objects,
// including raw inputs, before delegating to durabletask-go.
func (c *Client) ScheduleNewOrchestration(ctx context.Context, name string, opts ...api.NewOrchestrationOptions) (api.InstanceID, error) {
	if c.largePayloads != nil {
		opts = append(append([]api.NewOrchestrationOptions(nil), opts...), payloadValidationOption(api.WithRawInput("")))
	}
	return c.TaskHubGrpcClient.ScheduleNewOrchestration(ctx, name, opts...)
}

// RaiseEvent validates typed and raw event data before upstream offloading.
func (c *Client) RaiseEvent(ctx context.Context, id api.InstanceID, name string, opts ...api.RaiseEventOptions) error {
	if c.largePayloads != nil {
		opts = append(append([]api.RaiseEventOptions(nil), opts...), payloadValidationOption(api.WithRawEventData("")))
	}
	return c.TaskHubGrpcClient.RaiseEvent(ctx, id, name, opts...)
}

// TerminateOrchestration validates typed and raw output before upstream offloading.
func (c *Client) TerminateOrchestration(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error {
	if c.largePayloads != nil {
		opts = append(append([]api.TerminateOptions(nil), opts...), payloadValidationOption(api.WithRawOutput("")))
	}
	return c.TaskHubGrpcClient.TerminateOrchestration(ctx, id, opts...)
}

// SignalEntity validates typed and raw operation inputs before upstream offloading.
func (c *Client) SignalEntity(ctx context.Context, id api.EntityID, name string, opts ...api.SignalEntityOptions) error {
	if c.largePayloads != nil {
		opts = append(append([]api.SignalEntityOptions(nil), opts...), payloadValidationOption(api.WithRawSignalInput("")))
	}
	return c.TaskHubGrpcClient.SignalEntity(ctx, id, name, opts...)
}

// Dial connects to a Durable Task gRPC endpoint and returns a [Client] that
// owns the connection (close it with [Client.Close]). It uses insecure
// transport, suitable for a local sidecar / DTS emulator; supply your own
// connection via [NewClient] for TLS.
func Dial(endpoint string, opts ...ClientOption) (*Client, error) {
	c := &Client{}
	for _, opt := range opts {
		opt(c)
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("durabletask: dial %q: %w", endpoint, err)
	}
	c.connection = conn
	c.configurePayloads(c.largePayloads)
	c.conn = conn
	return c, nil
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
//
// This is one of the few methods declared here rather than inherited from the
// embedded client: connection ownership belongs to whoever dialled, and that
// is this package when the client came from [Dial] or from a durableClient
// binding.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// StartWorkItemListener is not supported under the Functions host.
//
// The embedded durabletask-go client offers this to drive a standalone worker
// that polls a backend for work. Under Functions the host owns dispatch: it
// invokes orchestrator and activity functions through the worker's trigger
// pipeline, and its durable gRPC endpoint does not serve work items at all.
// Register orchestrators and activities with [Middleware] instead.
func (c *Client) StartWorkItemListener(context.Context, *task.TaskRegistry) error {
	return errors.New("durabletask: StartWorkItemListener is not supported under the Functions " +
		"host, which dispatches orchestrations and activities itself; register them with " +
		"durabletask.Middleware() and pass it to app.Use")
}

// RuntimeStatus renders an orchestration's runtime status using the names
// Durable Functions uses everywhere else — "Pending", "Running", "Completed",
// "ContinuedAsNew", "Failed", "Terminated", "Suspended" — rather than the
// protobuf enum name durabletask-go exposes.
//
// [api.OrchestrationMetadata.RuntimeStatus]'s String method yields values like
// "ORCHESTRATION_STATUS_RUNNING". The host's own management API, the .NET
// worker's OrchestrationRuntimeStatus and the JavaScript worker all report the
// short form, so an app that echoes the enum name into a status response is
// inconsistent with every Durable Functions client and tool.
//
// This is a translation durabletask-go cannot perform, because it has no
// notion of running inside Durable Functions. That is why it lives here while
// the operations themselves are inherited unchanged.
func RuntimeStatus(m *api.OrchestrationMetadata) string {
	if m == nil {
		return ""
	}
	return runtimeStatusString(m.RuntimeStatus.String())
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
func defaultClientFromEnv(opts ...ClientOption) *Client {
	endpoint := os.Getenv(EnvGrpcEndpoint)
	if endpoint == "" {
		return nil
	}
	c, err := Dial(endpoint, opts...)
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
