// Package log implements the worker's logging pipeline: the wire
// translation between Go [log/slog] records and the host-side
// FunctionRpc [pb.RpcLog] message, plus the bootstrap stderr handler
// used before the gRPC stream is open.
//
// The package is internal to the worker in spirit (it exists to
// service the dispatcher and the otelfunc bridge), but lives in a
// regular path rather than under internal/ so middleware/otelfunc can
// import [RegisterObserver] without the worker package depending on
// any OTel module.
//
// # Record lifecycle
//
// Every user log record traverses three stages:
//
//  1. Construction. User code calls [slog.InfoContext] (or any other
//     slog method). The SDK's slog handler chain adds invocation_id,
//     function_name, and trigger_type from the [sdk.InvocationContext]
//     attached to the context.
//
//  2. gRPC emission. The chain bottoms out in a [Writer]-backed slog
//     handler ([NewUser] for user-category records, [NewSystem] for
//     worker-internal events). [Writer.Write] builds a [pb.RpcLog]
//     from the record and pushes it onto the dispatcher's outbound
//     gRPC stream. Host-supplied per-category log filters from
//     WorkerInitRequest.LogCategories are applied here.
//
//  3. Observer fan-out. After the RpcLog has been enqueued on the
//     outbound stream, [NewUser]'s handler clones the record and fans
//     it out to every [Observer] registered via [RegisterObserver].
//     The motivating consumer is the otelfunc package, which registers
//     an observer that bridges every user slog record into the
//     configured OpenTelemetry LoggerProvider so logs carry the same
//     trace.id and span.id as the worker invocation span.
//
// The bootstrap stage runs before stage 2 is wired up: during
// argument parsing and gRPC dial, slog records fall through to
// [NewBootstrap], which writes them to stderr with the
// LanguageWorkerConsoleLog prefix the host's stderr capture
// recognizes. Once the stream opens, worker.Start swaps the SDK
// default to a [Writer]-backed handler and bootstrap records stop.
//
// # Order-sensitive slog semantics
//
// The user and system slog handlers preserve the contract that
// attributes bound BEFORE a [slog.Logger.WithGroup] remain at the
// top level while attributes bound AFTER nest inside that group. The
// internal logComposer type tracks the bind-time group stack for each
// attribute, so chains like
//
//	slog.Default().
//	    With("tenant_id", t).
//	    WithGroup("http").
//	    With("method", m, "path", p)
//
// emit "tenant_id" at the top level and "method"/"path" inside the
// "http" group, matching slog.JSONHandler and slog.TextHandler.
//
// # Public surface
//
// Most callers only need:
//
//   - [NewBootstrap] - stderr handler used before the gRPC stream is open.
//   - [NewWriter]    - the gRPC RpcLog sender.
//   - [NewUser]      - user-category slog.Handler that wraps a Writer.
//   - [NewSystem]    - System-category slog.Handler for worker-internal logs.
//   - [RegisterObserver] / [Observer] - the otelfunc integration seam.
package log

import (
	"context"
	"log/slog"
	"math"
	"strconv"
	"strings"

	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// userHandler is the slog.Handler the SDK installs as the package-level
// default base via [sdk.SetDefaultBaseHandler] once the gRPC stream is
// open. It emits each record as a User-category RpcLog over the stream
// and then fans the record out to every registered [Observer].
//
// The SDK's outer handler (returned by [sdk.NewLogHandler]) is responsible
// for attaching invocation_id / function_name / trigger_type attributes
// from the InvocationContext. By the time records reach this handler they
// already carry those attrs in r.Attrs(); we surface invocation_id at the
// top level of the RpcLog (the host expects it there) and pack the rest
// into the JSON properties bag.
//
// The fan-out to observers handles the case where the host has
// WorkerOpenTelemetryEnabled=true and stops forwarding RpcLog User
// records to its own telemetry pipeline; without an observer hooked up,
// user logs would silently disappear in OTel mode. The otelfunc package
// registers an observer that bridges to the OTel LoggerProvider so OTel
// users get the right behavior automatically; users who don't import
// otelfunc never link any OTel code into their binary.
type userHandler struct {
	writer   *Writer
	composer logComposer
}

// NewUser returns the User-category gRPC slog.Handler. Called by
// worker.Start once the gRPC stream is open to install via
// sdk.SetDefaultBaseHandler.
func NewUser(w *Writer) slog.Handler {
	return &userHandler{writer: w}
}

func (h *userHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *userHandler) Handle(ctx context.Context, r slog.Record) error {
	rl := buildRpcLog(ctx, r, pb.RpcLog_User, h.composer)
	h.writer.Write(rl)

	// Fan out to registered observers (e.g. the otelfunc -> OTel
	// LoggerProvider bridge). Observers see the record post-RpcLog so
	// the user-facing host pipeline is the source of truth; observer
	// errors are non-fatal because the RpcLog has already gone out.
	if obs := observers.Load(); obs != nil {
		rec := r.Clone()
		// Replay any bound attrs onto the cloned record with their
		// recorded group path applied as a dotted prefix, so observers
		// see the same fully-qualified shape the RpcLog received. This
		// avoids each observer having to re-implement attr accumulation
		// or slog's order-sensitive Group/Attr semantics.
		for _, b := range h.composer.bound {
			rec.AddAttrs(slog.Attr{
				Key:   qualify(b.groups, b.attr.Key),
				Value: b.attr.Value,
			})
		}
		for _, fn := range *obs {
			fn(ctx, rec)
		}
	}
	return nil
}

func (h *userHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &userHandler{
		writer:   h.writer,
		composer: h.composer.withAttrs(attrs),
	}
}

func (h *userHandler) WithGroup(name string) slog.Handler {
	return &userHandler{
		writer:   h.writer,
		composer: h.composer.withGroup(name),
	}
}

// systemHandler is the slog.Handler used for worker-internal logs
// (dispatcher startup, message dispatch, function load, etc.). Records
// are emitted as System-category RpcLog values. A dedicated *slog.Logger
// instance is constructed at worker.Start time and stored on the
// dispatcher; worker code calls it via its SystemLogger() accessor.
type systemHandler struct {
	writer   *Writer
	composer logComposer
}

// NewSystem returns the System-category slog.Handler.
func NewSystem(w *Writer) slog.Handler {
	return &systemHandler{writer: w}
}

func (h *systemHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *systemHandler) Handle(ctx context.Context, r slog.Record) error {
	rl := buildRpcLog(ctx, r, pb.RpcLog_System, h.composer)
	// System logs use the "Worker" category by default if the record
	// does not specify one. The host's category filter recognizes
	// "Worker" as the catch-all for worker-side logs.
	if rl.Category == "" {
		rl.Category = "Worker"
	}
	h.writer.Write(rl)
	return nil
}

func (h *systemHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &systemHandler{
		writer:   h.writer,
		composer: h.composer.withAttrs(attrs),
	}
}

func (h *systemHandler) WithGroup(name string) slog.Handler {
	return &systemHandler{
		writer:   h.writer,
		composer: h.composer.withGroup(name),
	}
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
	c logComposer) *pb.RpcLog {

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
	// walk processes one attribute with its already-qualified key. We
	// pass the qualified key in rather than recomputing it inside the
	// closure so bound attrs (which use their bind-time group snapshot)
	// and record attrs (which use the full Handle-time group stack) can
	// share the same dispatch logic.
	walk := func(key string, a slog.Attr) {
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
	}
	// Bound attrs are qualified by the group stack that was open when
	// each was attached -- snapshotted by [logComposer.withAttrs] -- so
	// attrs added before a WithGroup remain unqualified by it, matching
	// the slog Handler contract.
	for _, b := range c.bound {
		walk(qualify(b.groups, b.attr.Key), b.attr)
	}
	// Record-time inline attrs are qualified by the full group stack at
	// Handle time -- they were emitted on a Record passed through the
	// most recent WithGroup, so they nest under it.
	r.Attrs(func(a slog.Attr) bool {
		walk(qualify(c.groups, a.Key), a)
		return true
	})
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
		val := v.Uint64()
		if val <= math.MaxInt64 {
			return &pb.TypedData{Data: &pb.TypedData_Int{Int: int64(val)}}
		}
		// uint64 value exceeds int64 range; represent as string to avoid overflow.
		return &pb.TypedData{Data: &pb.TypedData_String_{String_: strconv.FormatUint(val, 10)}}
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

// stringValue extracts a string representation from an slog.Value.
// The slog.Value.String() method handles all Go types sensibly (integers,
// bools, times, durations, etc.), so no special-case logic is required.
func stringValue(v slog.Value) string {
	return v.String()
}
