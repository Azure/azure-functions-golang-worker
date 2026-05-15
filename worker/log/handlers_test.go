package log

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

func TestBuildRpcLog_PopulatesPropertiesMap(t *testing.T) {
	// Verifies attrs land in the typed PropertiesMap field that the host
	// reads (RpcLog field 9), not the deprecated JSON Properties string
	// (field 7) that the host ignores for non-metric logs. Also verifies
	// the same attrs are rendered into Message in logfmt form so they
	// remain visible in Application Insights even though the host drops
	// PropertiesMap for user logs.
	ctx := sdk.NewContext(context.Background(), &sdk.InvocationContext{
		InvocationID: "inv-1",
		FunctionName: "fn",
	})

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "hello", 0)
	r.AddAttrs(
		slog.String("user_id", "u-42"),
		slog.Int("retry_count", 3),
		slog.Float64("latency_ms", 12.5),
		slog.Bool("cached", true),
	)

	rl := buildRpcLog(ctx, r, pb.RpcLog_User, logComposer{})

	if rl.Properties != "" {
		t.Errorf("RpcLog.Properties (deprecated string) should be empty, got %q", rl.Properties)
	}
	if len(rl.PropertiesMap) != 4 {
		t.Fatalf("expected 4 entries in PropertiesMap, got %d (%v)", len(rl.PropertiesMap), rl.PropertiesMap)
	}
	if got := rl.PropertiesMap["user_id"].GetString_(); got != "u-42" {
		t.Errorf("user_id: got %q want %q", got, "u-42")
	}
	if got := rl.PropertiesMap["retry_count"].GetInt(); got != 3 {
		t.Errorf("retry_count: got %d want 3", got)
	}
	if got := rl.PropertiesMap["latency_ms"].GetDouble(); got != 12.5 {
		t.Errorf("latency_ms: got %v want 12.5", got)
	}
	if got := rl.PropertiesMap["cached"].GetString_(); got != "true" {
		t.Errorf("cached: got %q want %q", got, "true")
	}

	// Message must contain the rendered logfmt form of every attr so
	// the host's message field carries the structured information all
	// the way to Application Insights.
	for _, want := range []string{"hello", "user_id=u-42", "retry_count=3", "latency_ms=12.5", "cached=true"} {
		if !strings.Contains(rl.Message, want) {
			t.Errorf("Message missing %q; got %q", want, rl.Message)
		}
	}
}

func TestBuildRpcLog_ReservedKeysSurfaceOnProto(t *testing.T) {
	// invocation_id, function_name, and event_id are surfaced on the
	// dedicated RpcLog proto fields (where the host expects them) rather
	// than the property bag.
	r := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	r.AddAttrs(
		slog.String("invocation_id", "inv-9"),
		slog.String("function_name", "fnX"),
		slog.String("event_id", "evt-42"),
		slog.String("normal_attr", "kept"),
	)

	rl := buildRpcLog(context.Background(), r, pb.RpcLog_User, logComposer{})

	if rl.InvocationId != "inv-9" {
		t.Errorf("InvocationId: got %q want %q", rl.InvocationId, "inv-9")
	}
	if rl.Category != "Function.fnX" {
		t.Errorf("Category: got %q want %q", rl.Category, "Function.fnX")
	}
	if rl.EventId != "evt-42" {
		t.Errorf("EventId: got %q want %q", rl.EventId, "evt-42")
	}
	if _, ok := rl.PropertiesMap["normal_attr"]; !ok {
		t.Errorf("PropertiesMap missing normal_attr; got keys=%v", keysOf(rl.PropertiesMap))
	}
	for _, reserved := range []string{"invocation_id", "function_name", "event_id"} {
		if _, ok := rl.PropertiesMap[reserved]; ok {
			t.Errorf("reserved key %q should not appear in PropertiesMap", reserved)
		}
	}
}

func TestSlogValueToTypedData(t *testing.T) {
	cases := []struct {
		name    string
		in      slog.Value
		wantStr string
		wantInt int64
		wantDbl float64
		kind    string
	}{
		{"string", slog.StringValue("hello"), "hello", 0, 0, "string"},
		{"int", slog.IntValue(42), "", 42, 0, "int"},
		{"int64", slog.Int64Value(-7), "", -7, 0, "int"},
		{"uint64", slog.Uint64Value(9), "", 9, 0, "int"},
		{"float", slog.Float64Value(3.14), "", 0, 3.14, "double"},
		{"bool_true", slog.BoolValue(true), "true", 0, 0, "string"},
		{"bool_false", slog.BoolValue(false), "false", 0, 0, "string"},
		{"duration", slog.DurationValue(2 * time.Second), "2s", 0, 0, "string"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			td := slogValueToTypedData(c.in)
			switch c.kind {
			case "string":
				if got := td.GetString_(); got != c.wantStr {
					t.Errorf("got string %q want %q", got, c.wantStr)
				}
			case "int":
				if got := td.GetInt(); got != c.wantInt {
					t.Errorf("got int %d want %d", got, c.wantInt)
				}
			case "double":
				if got := td.GetDouble(); got != c.wantDbl {
					t.Errorf("got double %v want %v", got, c.wantDbl)
				}
			}
		})
	}
}

func keysOf(m map[string]*pb.TypedData) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestRenderAttrsAsLogfmt(t *testing.T) {
	cases := []struct {
		name string
		in   []slog.Attr
		want string
	}{
		{
			name: "empty",
			in:   nil,
			want: "",
		},
		{
			name: "simple values",
			in: []slog.Attr{
				slog.String("user", "alice"),
				slog.Int("count", 7),
				slog.Bool("ok", true),
			},
			want: `user=alice count=7 ok=true`,
		},
		{
			name: "value with space gets quoted",
			in: []slog.Attr{
				slog.String("path", "/api/hello world"),
			},
			want: `path="/api/hello world"`,
		},
		{
			name: "value with equals gets quoted",
			in: []slog.Attr{
				slog.String("kv", "k=v"),
			},
			want: `kv="k=v"`,
		},
		{
			name: "empty string is empty quoted",
			in: []slog.Attr{
				slog.String("empty", ""),
			},
			want: `empty=""`,
		},
		{
			name: "float renders without trailing zeros",
			in: []slog.Attr{
				slog.Float64("latency_ms", 12.5),
			},
			want: `latency_ms=12.5`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := renderAttrsAsLogfmt(c.in)
			if got != c.want {
				t.Errorf("got %q want %q", got, c.want)
			}
		})
	}
}

func TestBuildRpcLog_RenderedTextHasReservedKeysAndUserAttrs(t *testing.T) {
	// Reserved keys (invocation_id, function_name, event_id, category)
	// must NOT appear in the rendered Message text since they are surfaced
	// on dedicated proto fields. Other attrs MUST appear so AI users can
	// grep on them in the message field.
	r := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	r.AddAttrs(
		slog.String("invocation_id", "should-not-render"),
		slog.String("function_name", "should-not-render"),
		slog.String("event_id", "should-not-render"),
		slog.String("category", "should-not-render"),
		slog.String("trigger_type", "httpTrigger"),
		slog.String("round_marker", "round1a-info"),
	)

	rl := buildRpcLog(context.Background(), r, pb.RpcLog_User, logComposer{})

	for _, banned := range []string{"invocation_id=", "function_name=", "event_id=", "category="} {
		if strings.Contains(rl.Message, banned) {
			t.Errorf("Message should not contain reserved key %q; got %q", banned, rl.Message)
		}
	}
	for _, want := range []string{"trigger_type=httpTrigger", "round_marker=round1a-info"} {
		if !strings.Contains(rl.Message, want) {
			t.Errorf("Message missing %q; got %q", want, rl.Message)
		}
	}
}

// TestBuildRpcLog_PreservesGroupAttrOrder asserts the slog Handler
// contract for With / WithGroup interleaving:
//
//   - attrs bound before a WithGroup remain at the top level (no prefix);
//   - attrs bound after a WithGroup are qualified by that group;
//   - inline record attrs are qualified by the full group stack at
//     Handle time, not the snapshot at any earlier With call.
//
// The pre-refactor implementation stored bound attrs and groups in two
// flat parallel slices and applied every group as a prefix to every
// bound attr, which violated all three invariants above. This test is
// the regression guard for that fix.
func TestBuildRpcLog_PreservesGroupAttrOrder(t *testing.T) {
	composer := logComposer{}.
		withAttrs([]slog.Attr{slog.String("tenant_id", "acme")}). // pre-group: top-level
		withGroup("http").
		withAttrs([]slog.Attr{ // post-group: qualified by http.
			slog.String("method", "POST"),
			slog.String("path", "/orders"),
		})

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "invocation finished", 0)
	r.AddAttrs(slog.Int("duration_ms", 142)) // inline: qualified by full stack (http.)

	rl := buildRpcLog(context.Background(), r, pb.RpcLog_User, composer)

	cases := []struct {
		key  string
		kind string // "string" or "int"
		want any
	}{
		{"tenant_id", "string", "acme"},
		{"http.method", "string", "POST"},
		{"http.path", "string", "/orders"},
		{"http.duration_ms", "int", int64(142)},
	}
	for _, c := range cases {
		got, ok := rl.PropertiesMap[c.key]
		if !ok {
			t.Errorf("PropertiesMap missing key %q; keys present: %v", c.key, keysOf(rl.PropertiesMap))
			continue
		}
		switch c.kind {
		case "string":
			if g := got.GetString_(); g != c.want {
				t.Errorf("%s: got %q want %q", c.key, g, c.want)
			}
		case "int":
			if g := got.GetInt(); g != c.want {
				t.Errorf("%s: got %d want %d", c.key, g, c.want)
			}
		}
	}
	// The forbidden flat-prefix shape would have produced "http.tenant_id"
	// (every group applied to every bound attr). Guard against that.
	if _, leaked := rl.PropertiesMap["http.tenant_id"]; leaked {
		t.Errorf("tenant_id should NOT be nested under http; PropertiesMap: %v", rl.PropertiesMap)
	}
}

// TestBuildRpcLog_NestedGroupsBindingTime asserts that attrs bound when
// only the outer group is open get the outer prefix, while attrs bound
// after the inner group opens get the dotted outer.inner prefix. Inline
// record attrs use the full stack.
func TestBuildRpcLog_NestedGroupsBindingTime(t *testing.T) {
	composer := logComposer{}.
		withGroup("outer").
		withAttrs([]slog.Attr{slog.Int("a", 1)}).
		withGroup("inner").
		withAttrs([]slog.Attr{slog.Int("b", 2)})

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	r.AddAttrs(slog.Int("c", 3))

	rl := buildRpcLog(context.Background(), r, pb.RpcLog_User, composer)

	cases := map[string]int64{
		"outer.a":       1,
		"outer.inner.b": 2,
		"outer.inner.c": 3,
	}
	for k, want := range cases {
		got, ok := rl.PropertiesMap[k]
		if !ok {
			t.Errorf("PropertiesMap missing %q; keys=%v", k, keysOf(rl.PropertiesMap))
			continue
		}
		if g := got.GetInt(); g != want {
			t.Errorf("%s: got %d want %d", k, g, want)
		}
	}
}

// TestBuildRpcLog_EmptyGroupIsNoop documents that withGroup("") is a
// no-op, matching slog's documented convention.
func TestBuildRpcLog_EmptyGroupIsNoop(t *testing.T) {
	composer := logComposer{}.
		withAttrs([]slog.Attr{slog.Int("a", 1)}).
		withGroup("").
		withAttrs([]slog.Attr{slog.Int("b", 2)})

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)

	rl := buildRpcLog(context.Background(), r, pb.RpcLog_User, composer)

	for _, k := range []string{"a", "b"} {
		if _, ok := rl.PropertiesMap[k]; !ok {
			t.Errorf("PropertiesMap missing top-level %q; keys=%v", k, keysOf(rl.PropertiesMap))
		}
	}
}

