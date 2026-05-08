package worker

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/log/noop"
)

// userLogHandler is the slog.Handler the SDK installs as the package-level
// default base via [sdk.SetDefaultBaseHandler] once the gRPC stream is
// open. It emits each record as a User-category RpcLog over the stream.
//
// The SDK's outer handler (returned by [sdk.NewLogHandler]) is responsible
// for attaching invocation_id / function_name / trigger_type attributes
// from the InvocationContext. By the time records reach this handler they
// already carry those attrs in r.Attrs(); we surface invocation_id at the
// top level of the RpcLog (the host expects it there) and pack the rest
// into the JSON properties bag.
//
// If a global OpenTelemetry LoggerProvider is configured (non-noop) the
// handler also forwards the record through the otelslog bridge so OTel-
// aware backends receive the same record. This handles the case where the
// host has WorkerOpenTelemetryEnabled=true and stops forwarding RpcLog
// User-category records to its own telemetry pipeline; without the bridge
// user logs would silently disappear in OTel mode.
type userLogHandler struct {
	writer *LogWriter
	attrs  []slog.Attr
	groups []string

	// otelBridge is the slog.Handler that fans the record out to the
	// global OTel LoggerProvider. Lazily resolved on first Handle call
	// (and atomically swapped in) so a user-supplied SetLoggerProvider
	// is picked up regardless of init ordering.
	otelBridge atomic.Pointer[slog.Handler]
}

// NewUserLogHandler returns the User-category gRPC slog.Handler. It is
// exported so callers (typically worker.Start) can install it via
// sdk.SetDefaultBaseHandler.
func newUserLogHandler(w *LogWriter) slog.Handler {
	return &userLogHandler{writer: w}
}

func (h *userLogHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *userLogHandler) Handle(ctx context.Context, r slog.Record) error {
	rl := buildRpcLog(ctx, r, pb.RpcLog_User, h.attrs, h.groups)
	h.writer.Write(rl)
	if bridge := h.resolveOTelBridge(); bridge != nil {
		// Forward the record (with the SDK-attached invocation attrs
		// intact) to the global OTel LoggerProvider. Errors are
		// non-fatal: RpcLog already received the record, so a failed
		// OTel emission only loses the OTel-side copy.
		_ = bridge.Handle(ctx, r.Clone())
	}
	return nil
}

// resolveOTelBridge returns the cached slog.Handler that bridges to the
// global OTel LoggerProvider, or nil if no provider is wired up. The
// resolution is performed at most once per handler instance after a real
// provider becomes visible — once cached we keep the bridge even if the
// global is later replaced, since the bridge re-resolves the underlying
// LoggerProvider on each call internally.
func (h *userLogHandler) resolveOTelBridge() slog.Handler {
	if cur := h.otelBridge.Load(); cur != nil {
		return *cur
	}
	if isNoopLoggerProvider() {
		return nil
	}
	bridge := slog.Handler(otelslog.NewHandler("github.com/azure/azure-functions-golang-worker/sdk"))
	if len(h.attrs) > 0 {
		bridge = bridge.WithAttrs(h.attrs)
	}
	for _, g := range h.groups {
		bridge = bridge.WithGroup(g)
	}
	h.otelBridge.CompareAndSwap(nil, &bridge)
	if cur := h.otelBridge.Load(); cur != nil {
		return *cur
	}
	return bridge
}

// isNoopLoggerProvider reports whether the global OpenTelemetry
// LoggerProvider is the noop default. The noop package's exported
// LoggerProvider type is the canonical signal -- we detect it via type
// assertion rather than calling logger methods to avoid spurious record
// allocations on every handle.
func isNoopLoggerProvider() bool {
	if _, ok := global.GetLoggerProvider().(noop.LoggerProvider); ok {
		return true
	}
	return false
}

func (h *userLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	cp := &userLogHandler{
		writer: h.writer,
		attrs:  mergeAttrs(h.attrs, attrs),
		groups: append([]string{}, h.groups...),
	}
	return cp
}

func (h *userLogHandler) WithGroup(name string) slog.Handler {
	cp := &userLogHandler{
		writer: h.writer,
		attrs:  append([]slog.Attr{}, h.attrs...),
		groups: append(append([]string{}, h.groups...), name),
	}
	return cp
}

// systemLogHandler is the slog.Handler used for worker-internal logs
// (dispatcher startup, message dispatch, function load, etc.). Records
// are emitted as System-category RpcLog values. A dedicated *slog.Logger
// instance is constructed at worker.Start time and stored on the
// dispatcher; worker code calls it via [systemLogger].
type systemLogHandler struct {
	writer *LogWriter
	attrs  []slog.Attr
	groups []string
}

// newSystemLogHandler returns the System-category slog.Handler.
func newSystemLogHandler(w *LogWriter) slog.Handler {
	return &systemLogHandler{writer: w}
}

func (h *systemLogHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *systemLogHandler) Handle(ctx context.Context, r slog.Record) error {
	rl := buildRpcLog(ctx, r, pb.RpcLog_System, h.attrs, h.groups)
	// System logs use the "Worker" category by default if the record
	// does not specify one. The host's category filter recognizes
	// "Worker" as the catch-all for worker-side logs.
	if rl.Category == "" {
		rl.Category = "Worker"
	}
	h.writer.Write(rl)
	return nil
}

func (h *systemLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	cp := *h
	cp.attrs = mergeAttrs(h.attrs, attrs)
	return &cp
}

func (h *systemLogHandler) WithGroup(name string) slog.Handler {
	cp := *h
	cp.groups = append(append([]string{}, h.groups...), name)
	return &cp
}

// buildRpcLog converts an slog.Record into an RpcLog proto carrying the
// given category. Invocation metadata stashed on the context via the SDK
// (invocation_id, function_name, trigger_type) is surfaced on the proto
// where the host expects it; remaining record attrs are rendered into
// the Message text (logfmt-style "key=value", matching slog.TextHandler)
// so they remain visible in the Application Insights message field, and
// also stored in PropertiesMap for proto-correctness.
//
// The host drops PropertiesMap for user logs (only the CustomMetric path
// reads it), so without text rendering the structured attributes would
// be silently lost on the worker→host hop. The .NET worker has the same
// limitation; here we take the slog.NewTextHandler convention of
// embedding attrs in the message and apply it to the RpcLog.Message
// field so Go users get the structured-logging experience they expect.
func buildRpcLog(ctx context.Context, r slog.Record, cat pb.RpcLog_RpcLogCategory,
	bound []slog.Attr, groups []string) *pb.RpcLog {

	rl := &pb.RpcLog{
		Level:       slogLevelToRpc(r.Level),
		Message:     r.Message,
		LogCategory: cat,
	}

	// Pull invocation metadata directly from the InvocationContext rather
	// than relying on the SDK's already-attached attrs, so this handler
	// behaves correctly even when the SDK wrapper is bypassed (e.g. when a
	// user constructs slog.Logger themselves but pipes records through
	// our base handler via slog.SetDefault).
	if ic, ok := sdk.FromContext(ctx); ok && ic != nil {
		rl.InvocationId = ic.InvocationID
		if ic.FunctionName != "" {
			rl.Category = "Function." + ic.FunctionName
		}
	}

	props := map[string]*pb.TypedData{}
	var renderable []slog.Attr
	walk := func(a slog.Attr) bool {
		key := a.Key
		if len(groups) > 0 {
			key = strings.Join(append(append([]string{}, groups...), key), ".")
		}
		switch key {
		case "invocation_id":
			if rl.InvocationId == "" {
				rl.InvocationId = stringValue(a.Value)
			}
		case "function_name":
			if rl.Category == "" {
				rl.Category = "Function." + stringValue(a.Value)
			}
		case "trigger_type":
			// Surfacing trigger_type at the proto level is unnecessary;
			// keep it in properties and the rendered text for diagnostic
			// consumers.
			props[key] = slogValueToTypedData(a.Value)
			renderable = append(renderable, slog.Attr{Key: key, Value: a.Value})
		case "category":
			rl.Category = stringValue(a.Value)
		case "event_id":
			rl.EventId = stringValue(a.Value)
		default:
			props[key] = slogValueToTypedData(a.Value)
			renderable = append(renderable, slog.Attr{Key: key, Value: a.Value})
		}
		return true
	}
	for _, a := range bound {
		walk(a)
	}
	r.Attrs(walk)
	if len(props) > 0 {
		rl.PropertiesMap = props
	}
	if suffix := renderAttrsAsLogfmt(renderable); suffix != "" {
		if rl.Message != "" {
			rl.Message = rl.Message + " " + suffix
		} else {
			rl.Message = suffix
		}
	}
	return rl
}

// renderAttrsAsLogfmt renders the given attrs into a space-separated
// "key=value" suffix matching the format slog.NewTextHandler emits.
// Values containing spaces, '=', '"', or control characters are quoted
// with strconv.Quote so the resulting string round-trips through Go's
// standard quoting rules.
func renderAttrsAsLogfmt(attrs []slog.Attr) string {
	if len(attrs) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, a := range attrs {
		if sb.Len() > 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(a.Key)
		sb.WriteByte('=')
		sb.WriteString(quoteIfNeeded(stringRender(a.Value)))
	}
	return sb.String()
}

// stringRender produces the canonical string form of an slog.Value the
// way slog.TextHandler would render it: native types format directly,
// everything else falls through to slog.Value.String().
func stringRender(v slog.Value) string {
	v = v.Resolve()
	switch v.Kind() {
	case slog.KindString:
		return v.String()
	case slog.KindInt64:
		return strconv.FormatInt(v.Int64(), 10)
	case slog.KindUint64:
		return strconv.FormatUint(v.Uint64(), 10)
	case slog.KindFloat64:
		return strconv.FormatFloat(v.Float64(), 'g', -1, 64)
	case slog.KindBool:
		return strconv.FormatBool(v.Bool())
	default:
		return v.String()
	}
}

// quoteIfNeeded wraps s in Go's standard double-quoted form when it
// contains characters that would break logfmt parsing or cause ambiguous
// rendering: control characters, spaces, '=', or '"'. Empty strings are
// always quoted as `""` so the key is preserved verbatim.
func quoteIfNeeded(s string) string {
	if s == "" {
		return `""`
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c <= ' ' || c == '=' || c == '"' {
			return strconv.Quote(s)
		}
	}
	return s
}

// slogValueToTypedData converts an slog.Value into the TypedData oneof
// shape the host expects. Strings, ints, floats, and bools map to their
// native TypedData kinds; everything else (durations, times, groups,
// LogValuers, and any unresolved kinds) falls back to the value's
// String() representation, matching what slog's default text handler
// would render.
func slogValueToTypedData(v slog.Value) *pb.TypedData {
	v = v.Resolve()
	switch v.Kind() {
	case slog.KindString:
		return &pb.TypedData{Data: &pb.TypedData_String_{String_: v.String()}}
	case slog.KindInt64:
		return &pb.TypedData{Data: &pb.TypedData_Int{Int: v.Int64()}}
	case slog.KindUint64:
		return &pb.TypedData{Data: &pb.TypedData_Int{Int: int64(v.Uint64())}}
	case slog.KindFloat64:
		return &pb.TypedData{Data: &pb.TypedData_Double{Double: v.Float64()}}
	case slog.KindBool:
		// TypedData has no Bool variant; render as the canonical
		// "true"/"false" string so the host preserves the value
		// faithfully (what most logging backends would do anyway).
		if v.Bool() {
			return &pb.TypedData{Data: &pb.TypedData_String_{String_: "true"}}
		}
		return &pb.TypedData{Data: &pb.TypedData_String_{String_: "false"}}
	default:
		return &pb.TypedData{Data: &pb.TypedData_String_{String_: v.String()}}
	}
}

// mergeAttrs concatenates two slog.Attr slices into a fresh backing array
// so handlers built from With(...) don't share state with the parent.
func mergeAttrs(a, b []slog.Attr) []slog.Attr {
	out := make([]slog.Attr, 0, len(a)+len(b))
	out = append(out, a...)
	out = append(out, b...)
	return out
}

// stringValue extracts a string from an slog.Value, handling the common
// kinds (String, Int64, Bool, Time) gracefully and falling back to the
// fmt-style representation otherwise.
func stringValue(v slog.Value) string {
	if v.Kind() == slog.KindString {
		return v.String()
	}
	return v.String()
}
