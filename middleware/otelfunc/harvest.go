// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package otelfunc

import (
	"github.com/azure/azure-functions-golang-worker/sdk"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// workerSetSpanAttributeKeys lists the span attribute keys this middleware
// itself sets on the worker invocation span. harvestSpanAttributesToOutbound
// filters these out so only user-authored attributes propagate to the
// host's parent activity. Matches the spirit of the dotnet-isolated
// worker's KnownAttributes filter (TraceConstants.cs).
var workerSetSpanAttributeKeys = map[attribute.Key]struct{}{
	"faas.invocation_id":                   {},
	"faas.name":                            {},
	"faas.trigger":                         {},
	"process.pid":                          {},
	"faas.instance":                        {},
	"azure.functions.live_logs_session_id": {},
}

// harvestSpanAttributesToOutbound copies user-set attributes from the
// worker invocation span onto the MiddlewareContext's outbound trace
// attribute set so the worker dispatcher forwards them on
// InvocationResponse.TraceContextAttributes. The host then applies each
// entry as a tag on its parent activity via Activity.AddTag, surfacing
// them on the host-emitted "request" record.
//
// Precedence: "fill if absent". Entries already on mc (e.g. because
// another middleware called SetOutboundTraceAttribute explicitly during
// the invocation) are preserved; the harvest only fills keys not yet set.
//
// Silently no-ops if the span doesn't implement [sdktrace.ReadOnlySpan]
// (noop / custom-impl TracerProviders) or if mc is nil.
func harvestSpanAttributesToOutbound(mc *sdk.MiddlewareContext, span trace.Span) {
	if mc == nil {
		return
	}
	ros, ok := span.(sdktrace.ReadOnlySpan)
	if !ok {
		return
	}
	existing := mc.OutboundTraceAttributes()
	for _, kv := range ros.Attributes() {
		if _, blocked := workerSetSpanAttributeKeys[kv.Key]; blocked {
			continue
		}
		key := string(kv.Key)
		if _, present := existing[key]; present {
			continue
		}
		mc.SetOutboundTraceAttribute(key, kv.Value.Emit())
		// Refresh the local view after the first lazy-alloc so the
		// "if present" check works for subsequent iterations within
		// this harvest pass too.
		existing = mc.OutboundTraceAttributes()
	}
}
