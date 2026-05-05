package sdk

import (
	"context"
	"testing"
)

func TestNewContext_RoundTrip(t *testing.T) {
	ic := &InvocationContext{
		InvocationID: "inv-1",
		FunctionID:   "fid-1",
		FunctionName: "Hello",
		TriggerType:  "httpTrigger",
	}

	ctx := NewContext(context.Background(), ic)

	got, ok := FromContext(ctx)
	if !ok {
		t.Fatal("expected ok=true from FromContext")
	}
	if got != ic {
		t.Errorf("expected same *InvocationContext, got %p (want %p)", got, ic)
	}
	if got.InvocationID != "inv-1" {
		t.Errorf("InvocationID mismatch: %q", got.InvocationID)
	}
}

func TestNewContext_NilParent(t *testing.T) {
	ic := &InvocationContext{InvocationID: "inv-x"}
	ctx := NewContext(nil, ic) //nolint:staticcheck // intentional nil check
	if ctx == nil {
		t.Fatal("expected non-nil ctx even when parent is nil")
	}
	if got, ok := FromContext(ctx); !ok || got.InvocationID != "inv-x" {
		t.Errorf("expected to read back inv-x, got ok=%v ic=%+v", ok, got)
	}
}

func TestFromContext_NilContext(t *testing.T) {
	ic, ok := FromContext(nil) //nolint:staticcheck // intentional nil check
	if ok || ic != nil {
		t.Errorf("expected (nil, false) from FromContext(nil); got (%+v, %v)", ic, ok)
	}
}

func TestFromContext_NoInvocation(t *testing.T) {
	ic, ok := FromContext(context.Background())
	if ok || ic != nil {
		t.Errorf("expected (nil, false) from a context that never held an InvocationContext; got (%+v, %v)", ic, ok)
	}
}

func TestInvocationContext_FieldsZeroValues(t *testing.T) {
	// Sanity check: zero-valued struct must be safe to read — middleware
	// authors expect every field to be readable without panics even when
	// the host omitted optional metadata (TraceContext, RetryContext).
	var ic InvocationContext
	if ic.TraceContext.TraceParent != "" {
		t.Error("zero TraceContext should have empty TraceParent")
	}
	if ic.RetryContext.RetryCount != 0 {
		t.Error("zero RetryContext should have RetryCount=0")
	}
	if ic.TriggerMetadata != nil {
		t.Error("zero TriggerMetadata should be nil (not a pre-allocated map)")
	}
}
