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
// User code should not normally reach for the wrapper. The intended
// pattern for user-visible behavior is to call the standard APIs (slog,
// span.SetAttributes via OpenTelemetry); middleware/otelfunc and the
// worker dispatcher coordinate through MiddlewareContext on the user's
// behalf. The wrapper is exported so authors of custom middleware can
// reach state the worker dispatcher needs (e.g. outbound trace
// attributes) without going through ad-hoc plumbing.
type MiddlewareContext struct {
	// InvocationContext is the user-facing trigger metadata for this
	// invocation. Embedded so callers can write mc.InvocationID rather
	// than mc.InvocationContext.InvocationID, matching Java's
	// MiddlewareContext-extends-ExecutionContext shape.
	*InvocationContext

	mu                 sync.Mutex
	outboundTraceAttrs map[string]string
}

// ContextWithMiddleware returns a context that carries the given
// MiddlewareContext.
//
// User code does not call this directly; the worker dispatcher builds the
// MiddlewareContext once per invocation, before the middleware chain
// runs. Tests and library authors who need to fabricate the carrier (for
// example, to unit-test a Middleware that writes outbound state) can
// call ContextWithMiddleware with a pre-built MC.
//
// Most test cases that don't care about MiddlewareContext-specific state
// should use [NewContext] instead — it wraps the given InvocationContext
// in a fresh MiddlewareContext implicitly.
//
// The name mirrors the standard library's context.WithValue /
// context.WithCancel convention: a function whose return type is a
// context.Context derived from parent, carrying the given value.
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

// SetOutboundTraceAttribute records a key/value pair on the per-
// invocation outbound trace attribute set. The worker dispatcher
// forwards the accumulated entries on InvocationResponse.
// TraceContextAttributes; the host applies each entry as a tag on its
// parent activity via Activity.AddTag(k, v), surfacing them as span
// attributes on the host-emitted "request" record in Application
// Insights.
//
// Intended for middleware integration (the middleware/otelfunc package
// calls this to forward harvested span attributes to the host's parent
// activity). User code that wants to tag the host's parent span should
// call span.SetAttributes(...) on the worker invocation span instead
// and let otelfunc auto-harvest.
//
// Safe to call from multiple goroutines (e.g. middleware that fans
// invocation work out to parallel sub-tasks).
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
// attributes for this invocation. Returns nil when no attributes have
// been recorded.
//
// Intended for the worker dispatcher's response builder. The returned
// map is the live backing store; callers that need an immutable
// snapshot should copy it. Reads are serialized against concurrent
// [SetOutboundTraceAttribute] writes via the same mutex, so the
// returned reference points at a stable map header.
func (mc *MiddlewareContext) OutboundTraceAttributes() map[string]string {
	if mc == nil {
		return nil
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.outboundTraceAttrs
}
