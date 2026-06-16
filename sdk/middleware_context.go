// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package sdk

import (
	"context"
	"sync"
)

// MiddlewareContext carries per-invocation state that flows through the
// worker's middleware chain. It is the framework / middleware-integration
// layer's working space: things the worker and middleware need to
// coordinate but that user-facing handlers don't normally interact with.
//
// MiddlewareContext embeds [*InvocationContext], so the trigger-side
// fields (InvocationID, FunctionName, TraceContext, etc.) are promoted
// and reachable directly:
//
//	mc, ok := sdk.MiddlewareContextFrom(ctx)
//	if ok {
//	    mc.SetOutboundTraceAttribute("tenant", tenant) // framework method
//	    fmt.Println(mc.FunctionName)                    // promoted from InvocationContext
//	}
//
// Authors of custom middleware can use the wrapper to reach state the
// worker dispatcher reads (e.g. outbound trace attributes). User code
// should call the standard observability APIs (slog, span.SetAttributes)
// instead; middleware/otelfunc coordinates through MiddlewareContext on
// the user's behalf.
type MiddlewareContext struct {
	*InvocationContext

	mu                 sync.Mutex
	outboundTraceAttrs map[string]string

	// inputBytes is the raw trigger input payload the host sent for this
	// invocation (the primary "in" binding's TypedData rendered to bytes).
	// The dispatcher populates it before the middleware chain runs so a
	// short-circuiting middleware (e.g. durable orchestration replay) can
	// read the raw payload without going through the reflective argument
	// binder. Nil when the trigger carried no input.
	inputBytes []byte

	// returnValue / hasReturnValue carry the value to encode into
	// InvocationResponse.ReturnValue. It is set either by the worker's
	// inner handler (the user function's first non-error return value) or
	// by a middleware that replaces execution entirely and produces the
	// response itself (mc.SetReturnValue). The dispatcher reads it when
	// building the response for non-HTTP triggers.
	returnValue    any
	hasReturnValue bool
}

// ContextWithMiddleware returns a context that carries the given
// MiddlewareContext. The name mirrors context.WithValue / context.WithCancel:
// a derived context.Context carrying the supplied value.
//
// The worker dispatcher calls this once per invocation, before the
// middleware chain runs. Most tests should use [NewContext] instead —
// it wraps the given InvocationContext in a fresh MiddlewareContext
// implicitly.
func ContextWithMiddleware(parent context.Context, mc *MiddlewareContext) context.Context {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithValue(parent, invocationContextKey{}, mc)
}

// MiddlewareContextFrom returns the *MiddlewareContext stored in ctx by
// the worker dispatcher, if any. The boolean is false when ctx was not
// produced by the dispatcher.
//
// Intended for middleware and framework integration code (e.g. the
// worker dispatcher reading recorded outbound trace attributes, or
// middleware/otelfunc writing them after harvest). User code that needs
// trigger metadata should use [FromContext] instead — it returns the
// embedded *InvocationContext directly.
func MiddlewareContextFrom(ctx context.Context) (*MiddlewareContext, bool) {
	if ctx == nil {
		return nil, false
	}
	mc, ok := ctx.Value(invocationContextKey{}).(*MiddlewareContext)
	return mc, ok && mc != nil
}

// SetOutboundTraceAttribute records a key/value pair to forward to the
// host on InvocationResponse.TraceContextAttributes. The host applies
// each entry as a tag on its parent activity via Activity.AddTag(k, v),
// surfacing them on the host-emitted "request" record in Application
// Insights.
//
// Intended for middleware integration. User code that wants to tag the
// host's parent span should call span.SetAttributes on the worker
// invocation span instead and let middleware/otelfunc auto-harvest.
//
// Safe to call from multiple goroutines.
func (mc *MiddlewareContext) SetOutboundTraceAttribute(key, value string) {
	if mc == nil {
		return
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.outboundTraceAttrs == nil {
		mc.outboundTraceAttrs = make(map[string]string, 4)
	}
	mc.outboundTraceAttrs[key] = value
}

// OutboundTraceAttributes returns the recorded outbound trace
// attributes, or nil when none have been recorded. The returned map is
// the live backing store; callers needing an immutable snapshot should
// copy it.
//
// Intended for the worker dispatcher's response builder.
func (mc *MiddlewareContext) OutboundTraceAttributes() map[string]string {
	if mc == nil {
		return nil
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.outboundTraceAttrs
}

// SetInputBytes stores the raw trigger input payload for this invocation.
//
// Intended for the worker dispatcher, which calls it once before the
// middleware chain runs. Middleware reads the value via [InputBytes].
func (mc *MiddlewareContext) SetInputBytes(b []byte) {
	if mc == nil {
		return
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.inputBytes = b
}

// InputBytes returns the raw trigger input payload the host sent for this
// invocation, or nil when the trigger carried no input.
//
// This is the seam a middleware that replaces function execution (for
// example, durable orchestration replay) uses to read the inbound payload
// (such as a base64-encoded orchestration history) without going through
// the reflective argument binder that feeds normal handler arguments.
func (mc *MiddlewareContext) InputBytes() []byte {
	if mc == nil {
		return nil
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.inputBytes
}

// SetReturnValue records the value the worker should encode into
// InvocationResponse.ReturnValue for this invocation.
//
// Two callers set it: the worker's inner handler records the user
// function's first non-error return value, and a middleware that replaces
// execution entirely records the response it produced (for example, the
// base64-encoded orchestrator actions from a durable replay). The latter
// is why this is exposed on MiddlewareContext rather than inferred solely
// from the handler's return: a short-circuiting middleware never calls the
// inner handler, so it needs an explicit way to set the response.
//
// HTTP triggers do not use this path; they encode their response through
// the ResponseWriter. Safe to call from multiple goroutines.
func (mc *MiddlewareContext) SetReturnValue(v any) {
	if mc == nil {
		return
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.returnValue = v
	mc.hasReturnValue = true
}

// ReturnValue returns the recorded return value and whether one was set.
// Intended for the worker dispatcher's response builder.
func (mc *MiddlewareContext) ReturnValue() (any, bool) {
	if mc == nil {
		return nil, false
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.returnValue, mc.hasReturnValue
}
