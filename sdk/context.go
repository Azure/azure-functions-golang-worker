package sdk

import "context"

// InvocationContext carries metadata about the in-flight function invocation.
//
// It is populated by the worker dispatcher from the gRPC InvocationRequest at
// the start of every invocation, stored on the standard context.Context, and
// retrieved inside user handler code via [FromContext].
//
// Layout matches what other Azure Functions out-of-process workers expose
// (Python's Context, Java's ExecutionContext, .NET's FunctionContext) and
// follows the AWS Lambda lambdacontext.LambdaContext convention of stashing
// per-invocation metadata on context.Context for idiomatic Go consumption.
type InvocationContext struct {
	// InvocationID is the unique identifier for this invocation, assigned by
	// the Functions host.
	InvocationID string

	// FunctionID is the stable identifier the host uses for this function
	// definition. The same FunctionID is used across all invocations of a
	// given function within the worker's lifetime.
	FunctionID string

	// FunctionName is the user-facing name of the function (e.g. "hello").
	FunctionName string

	// TriggerType identifies the binding type that triggered this invocation
	// (e.g. "httpTrigger", "timerTrigger", "blobTrigger"). Useful for
	// telemetry classification (OpenTelemetry's faas.trigger attribute) and
	// for middleware that wants to behave differently per trigger.
	TriggerType string

	// TraceContext carries the W3C trace context the host attached to the
	// invocation. It is the bridge for distributed-tracing correlation
	// between the Functions host span and any spans produced inside the Go
	// worker (see sdk/otelfunc.Middleware for the integration point).
	TraceContext TraceContext

	// RetryContext describes the retry state of the invocation, populated
	// when the host has applied a retry policy.
	RetryContext RetryContext

	// TriggerMetadata is the bag of host-supplied metadata associated with
	// the trigger (e.g. blob name, queue insertion time, HTTP route
	// parameters). Values are flattened to strings for ergonomic access;
	// non-string TypedData values are skipped.
	TriggerMetadata map[string]string
}

// TraceContext mirrors the fields of pb.RpcTraceContext that the Functions
// host sends. Stored on InvocationContext rather than directly on
// context.Context so that the same instance is observable through both
// FromContext and middleware's *InvocationContext parameter.
type TraceContext struct {
	// TraceParent is the W3C traceparent header value, e.g.
	// "00-{trace-id}-{span-id}-{flags}".
	TraceParent string

	// TraceState is the W3C tracestate header value (vendor-specific
	// extensions).
	TraceState string

	// Attributes carries any extra attributes the host attached to the
	// trace context. Typically empty for inbound invocations.
	Attributes map[string]string
}

// RetryContext describes the host-applied retry state for an invocation.
//
// When no retry policy is configured for the function, RetryCount and
// MaxRetryCount are both zero.
type RetryContext struct {
	// RetryCount is the current retry attempt (0 on the first invocation,
	// 1 on the first retry, etc.).
	RetryCount int32

	// MaxRetryCount is the configured maximum number of retries. A value of
	// 0 means no retry policy is in effect.
	MaxRetryCount int32
}

// invocationContextKey is the unexported context key used for stashing
// *InvocationContext. The empty-struct-key pattern prevents collisions with
// other packages and is unallocatable.
type invocationContextKey struct{}

// NewContext returns a new context that carries the given *InvocationContext.
//
// User code does not normally call NewContext directly; the worker dispatcher
// constructs the per-invocation context before invoking the middleware chain.
// Tests and library authors who need to fabricate an invocation context (for
// example, to unit-test a Middleware in isolation) can use NewContext to
// produce a context with predetermined values.
func NewContext(parent context.Context, ic *InvocationContext) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithValue(parent, invocationContextKey{}, ic)
}

// FromContext returns the *InvocationContext stored in ctx, if any.
//
// The boolean is false when ctx was not produced by the worker dispatcher
// (for example, in unit tests that pass context.Background() directly to a
// handler). User code should check the boolean and handle the missing case
// gracefully.
func FromContext(ctx context.Context) (*InvocationContext, bool) {
	if ctx == nil {
		return nil, false
	}
	ic, ok := ctx.Value(invocationContextKey{}).(*InvocationContext)
	return ic, ok
}
