package sdk

import "testing"

// The dispatcher hands text payloads over as strings because assigning a
// string copies a header, not the payload. Converting to bytes is the only
// step that allocates, so it must not happen until something asks for it.
func TestMiddlewareContext_TextInputIsNotConvertedUntilRead(t *testing.T) {
	mc := &MiddlewareContext{}
	mc.SetInputString("orchestration-history")

	if got := mc.InputString(); got != "orchestration-history" {
		t.Errorf("InputString() = %q, want the payload back", got)
	}

	// Reading the text form must not have produced a byte copy.
	mc.mu.Lock()
	converted := mc.inputBytes != nil
	mc.mu.Unlock()
	if converted {
		t.Error("reading InputString should not convert the payload to bytes")
	}

	if got := string(mc.InputBytes()); got != "orchestration-history" {
		t.Errorf("InputBytes() = %q, want the payload back", got)
	}

	// And the conversion is cached rather than repeated.
	mc.mu.Lock()
	cached := mc.inputBytes
	mc.mu.Unlock()
	if cached == nil {
		t.Fatal("expected the conversion to be cached")
	}
	if second := mc.InputBytes(); &second[0] != &cached[0] {
		t.Error("expected InputBytes to reuse the cached conversion")
	}
}

// A binary payload is aliased, never copied, and is readable as text too.
func TestMiddlewareContext_BinaryInputIsAliased(t *testing.T) {
	payload := []byte("raw-bytes")
	mc := &MiddlewareContext{}
	mc.SetInputBytes(payload)

	got := mc.InputBytes()
	if len(got) == 0 || &got[0] != &payload[0] {
		t.Error("expected the byte payload to be aliased, not copied")
	}
	if s := mc.InputString(); s != "raw-bytes" {
		t.Errorf("InputString() = %q, want the payload rendered as text", s)
	}
}

// A trigger that carried no payload reads as empty either way.
func TestMiddlewareContext_NoInput(t *testing.T) {
	mc := &MiddlewareContext{}

	if got := mc.InputBytes(); got != nil {
		t.Errorf("InputBytes() = %v, want nil", got)
	}
	if got := mc.InputString(); got != "" {
		t.Errorf("InputString() = %q, want empty", got)
	}
}

// Auxiliary binding payloads follow the same rule: text is held as a string
// and converted on first read.
func TestMiddlewareContext_BindingTextIsConvertedOnDemand(t *testing.T) {
	mc := &MiddlewareContext{}
	mc.SetBindingInputString("durableClient", `{"rpcBaseUrl":"http://127.0.0.1:1234"}`)

	mc.mu.Lock()
	converted := mc.bindingInputs != nil
	mc.mu.Unlock()
	if converted {
		t.Error("storing a binding string should not convert it")
	}

	b, ok := mc.BindingInput("durableClient")
	if !ok {
		t.Fatal("expected the binding to be present")
	}
	if string(b) != `{"rpcBaseUrl":"http://127.0.0.1:1234"}` {
		t.Errorf("BindingInput = %q", b)
	}

	// Cached, so a second read returns the same slice.
	again, _ := mc.BindingInput("durableClient")
	if &again[0] != &b[0] {
		t.Error("expected BindingInput to reuse the cached conversion")
	}

	if _, ok := mc.BindingInput("missing"); ok {
		t.Error("expected a missing binding to report absent")
	}
}

// Byte bindings are aliased rather than copied.
func TestMiddlewareContext_BindingBytesAreAliased(t *testing.T) {
	payload := []byte("binding-bytes")
	mc := &MiddlewareContext{}
	mc.SetBindingInput("blob", payload)

	got, ok := mc.BindingInput("blob")
	if !ok {
		t.Fatal("expected the binding to be present")
	}
	if &got[0] != &payload[0] {
		t.Error("expected the byte payload to be aliased, not copied")
	}
}

// The accessors are nil-safe, matching the rest of MiddlewareContext.
func TestMiddlewareContext_NilReceiverIsSafe(t *testing.T) {
	var mc *MiddlewareContext

	mc.SetInputString("x")
	mc.SetInputBytes([]byte("x"))
	mc.SetBindingInputString("n", "x")
	mc.SetBindingInput("n", []byte("x"))

	if got := mc.InputString(); got != "" {
		t.Errorf("InputString() = %q, want empty", got)
	}
	if got := mc.InputBytes(); got != nil {
		t.Errorf("InputBytes() = %v, want nil", got)
	}
	if _, ok := mc.BindingInput("n"); ok {
		t.Error("expected no binding on a nil context")
	}
}
