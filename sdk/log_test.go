package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

func TestNewLogHandler_AttachesInvocationAttrs(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(NewLogHandler(base))

	ic := &InvocationContext{
		InvocationID: "inv-42",
		FunctionName: "Hello",
		TriggerType:  "httpTrigger",
	}
	ctx := NewContext(context.Background(), ic)

	logger.InfoContext(ctx, "ping")

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("log output is not valid JSON: %v\n%s", err, buf.String())
	}
	if rec["invocation_id"] != "inv-42" {
		t.Errorf("invocation_id mismatch: %v", rec["invocation_id"])
	}
	if rec["function_name"] != "Hello" {
		t.Errorf("function_name mismatch: %v", rec["function_name"])
	}
	if rec["trigger_type"] != "httpTrigger" {
		t.Errorf("trigger_type mismatch: %v", rec["trigger_type"])
	}
	if rec["msg"] != "ping" {
		t.Errorf("msg mismatch: %v", rec["msg"])
	}
}

func TestNewLogHandler_NoInvocationContext(t *testing.T) {
	// Records emitted outside an invocation must not get phantom
	// invocation attributes — just behave like the base handler.
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, nil)
	logger := slog.New(NewLogHandler(base))

	logger.Info("standalone")

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("log output is not valid JSON: %v", err)
	}
	if _, present := rec["invocation_id"]; present {
		t.Errorf("invocation_id should not be present when no IC on context: %v", rec)
	}
}

func TestNewLogHandler_OmitsEmptyFields(t *testing.T) {
	// Half-populated InvocationContext: only invocation_id is set.
	// Other fields must be omitted from the log record entirely (not
	// serialized as empty strings) to keep log volumes tidy.
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, nil)
	logger := slog.New(NewLogHandler(base))

	ctx := NewContext(context.Background(), &InvocationContext{InvocationID: "only-id"})
	logger.InfoContext(ctx, "msg")

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("log output is not valid JSON: %v", err)
	}
	if rec["invocation_id"] != "only-id" {
		t.Errorf("invocation_id mismatch: %v", rec["invocation_id"])
	}
	for _, key := range []string{"function_name", "trigger_type"} {
		if _, present := rec[key]; present {
			t.Errorf("expected %q to be omitted; got %v", key, rec[key])
		}
	}
}

func TestNewLogHandler_PreservesUserAttrs(t *testing.T) {
	// User-supplied attrs (via With) and per-call attrs must coexist with
	// the injected invocation attributes.
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, nil)
	logger := slog.New(NewLogHandler(base)).With("service", "demo")

	ctx := NewContext(context.Background(), &InvocationContext{InvocationID: "inv-77"})
	logger.InfoContext(ctx, "event", "key", "value")

	out := buf.String()
	for _, want := range []string{`"service":"demo"`, `"key":"value"`, `"invocation_id":"inv-77"`} {
		if !strings.Contains(out, want) {
			t.Errorf("log output missing %q\n%s", want, out)
		}
	}
}

func TestNewLogHandler_NilBaseDefaultsToStderr(t *testing.T) {
	// Smoke test: passing nil base must not panic.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("NewLogHandler(nil) panicked: %v", r)
		}
	}()
	if h := NewLogHandler(nil); h == nil {
		t.Fatal("expected non-nil handler from NewLogHandler(nil)")
	}
}

func TestNewLogHandler_PicksUpDefaultBaseHandlerSwap(t *testing.T) {
	// The whole point of the swappable default base is that handlers
	// constructed before the swap pick up the new base on the next log
	// call. This is the contract the worker package depends on to upgrade
	// the bootstrap stderr handler to the gRPC-routing handler once the
	// stream is up.
	t.Cleanup(func() { SetDefaultBaseHandler(nil) })

	first := &countingHandler{}
	SetDefaultBaseHandler(first)

	logger := slog.New(NewLogHandler(nil))
	logger.Info("before swap")
	if first.count() != 1 {
		t.Fatalf("first handler should have received 1 record; got %d", first.count())
	}

	second := &countingHandler{}
	SetDefaultBaseHandler(second)

	logger.Info("after swap")
	if second.count() != 1 {
		t.Errorf("second handler should have received 1 record after swap; got %d", second.count())
	}
	if first.count() != 1 {
		t.Errorf("first handler should remain at 1 record after swap; got %d", first.count())
	}
}

func TestNewLogHandler_ExplicitBaseBypassesDefault(t *testing.T) {
	// When the caller passes a non-nil base, NewLogHandler must honor it
	// regardless of any package-level default that may be installed.
	t.Cleanup(func() { SetDefaultBaseHandler(nil) })

	pkgDefault := &countingHandler{}
	SetDefaultBaseHandler(pkgDefault)

	explicit := &countingHandler{}
	logger := slog.New(NewLogHandler(explicit))
	logger.Info("hi")

	if explicit.count() != 1 {
		t.Errorf("explicit base should receive the record; got %d", explicit.count())
	}
	if pkgDefault.count() != 0 {
		t.Errorf("package default should be bypassed; got %d records", pkgDefault.count())
	}
}

func TestNewLogHandler_WithAttrsPreservedAcrossSwap(t *testing.T) {
	// User code calls .With(...) on a logger; the resulting handler must
	// continue to apply the bound attributes when the package-level base
	// is swapped underneath.
	t.Cleanup(func() { SetDefaultBaseHandler(nil) })

	cap := &captureHandler{}
	SetDefaultBaseHandler(cap)

	logger := slog.New(NewLogHandler(nil)).With("service", "demo")
	logger.Info("ping")

	if cap.lastAttrs == nil {
		t.Fatal("captured handler did not see any attrs")
	}
	if got := cap.lastAttrs["service"]; got != "demo" {
		t.Errorf("service attr lost: got %v, want demo", got)
	}
}

// countingHandler is a tiny stub that records the number of Handle calls.
type countingHandler struct {
	mu   sync.Mutex
	cnt  int
}

func (h *countingHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cnt
}
func (h *countingHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *countingHandler) Handle(_ context.Context, _ slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cnt++
	return nil
}
func (h *countingHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *countingHandler) WithGroup(_ string) slog.Handler      { return h }

// captureHandler is a stub slog.Handler that records every record's
// (combined) attrs on the root instance — including attrs bound via
// WithAttrs higher in the chain. Used to validate that the wrapper's
// accumulated With state survives a base swap.
type captureHandler struct {
	root      *captureHandler
	bound     []slog.Attr
	mu        sync.Mutex
	lastAttrs map[string]any
}

func (h *captureHandler) self() *captureHandler {
	if h.root == nil {
		return h
	}
	return h.root
}

func (h *captureHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	root := h.self()
	root.mu.Lock()
	defer root.mu.Unlock()
	root.lastAttrs = map[string]any{}
	for _, a := range h.bound {
		root.lastAttrs[a.Key] = a.Value.Any()
	}
	r.Attrs(func(a slog.Attr) bool {
		root.lastAttrs[a.Key] = a.Value.Any()
		return true
	})
	return nil
}
func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	cp := &captureHandler{root: h.self()}
	cp.bound = append(cp.bound, h.bound...)
	cp.bound = append(cp.bound, attrs...)
	return cp
}
func (h *captureHandler) WithGroup(_ string) slog.Handler { return h }

// TestNewLogHandler_PreservesGroupAttrOrder asserts that attrs bound via
// With BEFORE a WithGroup remain at the top level, while attrs bound
// AFTER and inline record attrs nest under the group. This is the slog
// Handler contract -- WithGroup applies only to subsequently-added
// attributes. The original invocationLogHandler stored attrs and groups
// in two flat parallel slices and applied all groups before all attrs,
// which collapsed every bound attr under every later group; this test
// is the regression guard for that fix.
func TestNewLogHandler_PreservesGroupAttrOrder(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, nil)
	logger := slog.New(NewLogHandler(base)).
		With("tenant_id", "acme").
		WithGroup("http").
		With("method", "POST", "path", "/orders")

	logger.Info("invocation finished", "duration_ms", 142)

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("log output is not valid JSON: %v\n%s", err, buf.String())
	}

	if rec["tenant_id"] != "acme" {
		t.Errorf("tenant_id should be top-level; got %v\nfull record: %s", rec["tenant_id"], buf.String())
	}
	httpGroup, ok := rec["http"].(map[string]any)
	if !ok {
		t.Fatalf("http group missing or not a map: %v", rec["http"])
	}
	if httpGroup["method"] != "POST" {
		t.Errorf("http.method mismatch: %v", httpGroup["method"])
	}
	if httpGroup["path"] != "/orders" {
		t.Errorf("http.path mismatch: %v", httpGroup["path"])
	}
	// duration_ms is an inline record attr emitted after the group is
	// open, so slog spec puts it inside the group.
	if httpGroup["duration_ms"] == nil {
		t.Errorf("http.duration_ms missing; got %v", httpGroup)
	}
	// tenant_id should NOT be nested inside http.
	if _, leaked := httpGroup["tenant_id"]; leaked {
		t.Errorf("tenant_id leaked into http group: %v", httpGroup)
	}
}

// TestNewLogHandler_SDKAttrsTopLevelAcrossWithGroup asserts that the
// SDK-attached invocation attrs (invocation_id, function_name,
// trigger_type) stay at the top level of the emitted record even when
// the user logger has called WithGroup. The host's RpcLog proto
// extraction and customer queries against the top-level fields would
// otherwise silently fail when users follow the slog idiom of grouping
// related fields under a namespace.
func TestNewLogHandler_SDKAttrsTopLevelAcrossWithGroup(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, nil)
	logger := slog.New(NewLogHandler(base)).WithGroup("http")

	ctx := NewContext(context.Background(), &InvocationContext{
		InvocationID: "inv-99",
		FunctionName: "Hello",
		TriggerType:  "httpTrigger",
	})
	logger.InfoContext(ctx, "ping", "status", 200)

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("log output is not valid JSON: %v\n%s", err, buf.String())
	}

	if rec["invocation_id"] != "inv-99" {
		t.Errorf("invocation_id should be top-level despite WithGroup; got %v\nfull record: %s",
			rec["invocation_id"], buf.String())
	}
	if rec["function_name"] != "Hello" {
		t.Errorf("function_name should be top-level: %v", rec["function_name"])
	}
	if rec["trigger_type"] != "httpTrigger" {
		t.Errorf("trigger_type should be top-level: %v", rec["trigger_type"])
	}
	// The user-supplied inline attr does belong inside the group.
	httpGroup, ok := rec["http"].(map[string]any)
	if !ok {
		t.Fatalf("http group missing: %v", rec["http"])
	}
	if httpGroup["status"] == nil {
		t.Errorf("http.status missing; got %v", httpGroup)
	}
}

// TestNewLogHandler_NestedGroups asserts that nested groups produce
// nested JSON objects, with attrs landing in whichever group was open
// when they were bound.
func TestNewLogHandler_NestedGroups(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, nil)
	logger := slog.New(NewLogHandler(base)).
		WithGroup("outer").
		With("a", 1).
		WithGroup("inner").
		With("b", 2)

	logger.Info("msg")

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("log output is not valid JSON: %v\n%s", err, buf.String())
	}
	outer, ok := rec["outer"].(map[string]any)
	if !ok {
		t.Fatalf("outer missing: %v", rec)
	}
	if outer["a"] == nil {
		t.Errorf("outer.a missing: %v", outer)
	}
	inner, ok := outer["inner"].(map[string]any)
	if !ok {
		t.Fatalf("outer.inner missing: %v", outer)
	}
	if inner["b"] == nil {
		t.Errorf("outer.inner.b missing: %v", inner)
	}
}

// TestNewLogHandler_EmptyGroupIsNoop documents that an empty WithGroup
// name is dropped silently, matching slog's documented convention. This
// keeps round-tripped chain construction (e.g. `.WithGroup(maybeEmpty)`)
// safe without forcing callers to nil-check at every site.
func TestNewLogHandler_EmptyGroupIsNoop(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, nil)
	logger := slog.New(NewLogHandler(base)).
		With("a", 1).
		WithGroup("").
		With("b", 2)

	logger.Info("msg")

	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("log output is not valid JSON: %v\n%s", err, buf.String())
	}
	if rec["a"] == nil {
		t.Errorf("a missing: %v", rec)
	}
	if rec["b"] == nil {
		t.Errorf("b missing: %v", rec)
	}
	if _, present := rec[""]; present {
		t.Errorf("empty group should not have produced a top-level empty key: %v", rec)
	}
}

