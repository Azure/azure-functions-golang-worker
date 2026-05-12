package sdk

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"
)

// NewLogHandler returns an slog.Handler that automatically attaches the
// current invocation's metadata (invocation_id, function_name,
// trigger_type) as attributes to every record processed inside an
// invocation context.
//
// The handler delegates to a base handler. If base is nil, the package-level
// default base handler is consulted at log time. The default starts out as a
// JSON handler over stderr — suitable for the worker's bootstrap phase
// before the gRPC stream is up. Once worker.Start connects to the host, the
// worker package swaps the default for one that emits RpcLog over gRPC via
// [SetDefaultBaseHandler]; existing handlers returned from NewLogHandler
// pick up the swap automatically.
//
// Pass an explicit non-nil base when you want to bypass the default, for
// example in tests that capture log output to a buffer:
//
//	var buf bytes.Buffer
//	logger := slog.New(sdk.NewLogHandler(slog.NewJSONHandler(&buf, nil)))
//
// The wrapper plays well with [slog.Logger.With], [slog.Logger.WithGroup],
// and slog's structured-attribute APIs: those operations accumulate state
// on the wrapper and are applied to the resolved base handler at log time
// so that base swaps remain effective.
//
// Inspired by aws-lambda-go's lambdacontext.NewLogHandler — the design
// pattern is "wrap the user's slog.Handler, look up invocation metadata via
// FromContext, and add it as record attributes."
//
// Example:
//
//	slog.SetDefault(slog.New(sdk.NewLogHandler(nil)))
//
//	func TimerHandler(ctx context.Context, t bindings.TimerInfo) error {
//	    slog.InfoContext(ctx, "timer fired", "schedule", t.ScheduleStatus)
//	    // record now carries invocation_id, function_name, trigger_type
//	    return nil
//	}
//
// Usually you don't need to call NewLogHandler at all — sdk's init function
// installs the resulting handler as the slog default during package
// initialization, so simply importing the sdk package is enough to make
// slog.InfoContext route through Functions logging. See sdk/init.go for the
// auto-install detection logic.
func NewLogHandler(base slog.Handler) slog.Handler {
	return &invocationLogHandler{explicitBase: base}
}

// NewLogger is shorthand for slog.New(NewLogHandler(nil)). The returned
// logger's handler defers to the package-level default base, which is
// upgraded by the worker package once the gRPC stream is open.
//
//	slog.SetDefault(sdk.NewLogger())
func NewLogger() *slog.Logger {
	return slog.New(NewLogHandler(nil))
}

// SetDefaultBaseHandler installs the slog.Handler used by [NewLogHandler]
// when the caller does not pass an explicit base. The worker package calls
// it once the gRPC stream is open to upgrade the bootstrap stderr handler
// to one that emits RpcLog over gRPC.
//
// Calling SetDefaultBaseHandler with nil restores the bootstrap behavior:
// records are emitted as JSON to stderr.
//
// User code should not normally call this. To install a fully custom
// slog.Handler, set it via slog.SetDefault and the sdk auto-install will
// be bypassed.
func SetDefaultBaseHandler(h slog.Handler) {
	if h == nil {
		baseHolder.Store(nil)
		return
	}
	baseHolder.Store(&h)
}

// baseHolder is a swappable pointer to the package-level default base
// handler. Loaded on every Handle call so the worker can install a
// gRPC-routing handler at runtime without invalidating already-constructed
// invocationLogHandler instances.
var baseHolder atomic.Pointer[slog.Handler]

// defaultBase returns the package-level default base handler. Falls back
// to a stderr JSON handler when none is installed (the worker package
// installs a gRPC-routing handler in its bootstrap; this fallback is for
// SDK consumers running outside the worker, e.g. unit tests).
func defaultBase() slog.Handler {
	if p := baseHolder.Load(); p != nil {
		return *p
	}
	return slog.NewJSONHandler(os.Stderr, nil)
}

// invocationLogHandler is the slog.Handler implementation returned by
// NewLogHandler. It accumulates state from With/WithGroup calls and applies
// it to the resolved base at log time so the base can be swapped at any
// point during the worker's lifecycle.
type invocationLogHandler struct {
	// explicitBase, when non-nil, takes precedence over the package-level
	// default. Used by callers (e.g. tests) that want to bypass the default.
	explicitBase slog.Handler
	// ops is the interleaved log of WithAttrs / WithGroup calls made
	// against this handler chain, in the order they were issued. We
	// replay it against the resolved base at Handle time so the base
	// can be swapped at any point and so slog's order-sensitive
	// Group/Attr contract is preserved: attrs added before a WithGroup
	// stay outside that group; attrs added after are nested under it.
	ops []logOp
}

// logOp is one step in the With / WithGroup history of an
// [invocationLogHandler] chain. Exactly one of attrs / group is
// populated, signaled by isGroup.
type logOp struct {
	isGroup bool
	group   string
	attrs   []slog.Attr
}

// resolveBase picks the handler to delegate to: the explicit base when set,
// otherwise the package-level default.
func (h *invocationLogHandler) resolveBase() slog.Handler {
	if h.explicitBase != nil {
		return h.explicitBase
	}
	return defaultBase()
}

// effective composes the resolved base with the SDK's invocation
// attributes (when an InvocationContext is on the context) and then
// replays the user's With / WithGroup ops in their original order.
//
// SDK invocation attrs (invocation_id, function_name, trigger_type) are
// attached BEFORE the user ops, so they always land at the top level of
// the emitted record -- even if the user logger has called WithGroup.
// Without that ordering, an `slog.Default().WithGroup("http").Info(...)`
// call would nest invocation_id under "http", which makes the host's
// proto extraction (and customer queries against the AI/NR top-level
// invocation_id field) silently fail.
func (h *invocationLogHandler) effective(ctx context.Context) slog.Handler {
	b := h.resolveBase()
	if ic, ok := FromContext(ctx); ok && ic != nil {
		var sdkAttrs []slog.Attr
		if ic.InvocationID != "" {
			sdkAttrs = append(sdkAttrs, slog.String("invocation_id", ic.InvocationID))
		}
		if ic.FunctionName != "" {
			sdkAttrs = append(sdkAttrs, slog.String("function_name", ic.FunctionName))
		}
		if ic.TriggerType != "" {
			sdkAttrs = append(sdkAttrs, slog.String("trigger_type", ic.TriggerType))
		}
		if len(sdkAttrs) > 0 {
			b = b.WithAttrs(sdkAttrs)
		}
	}
	for _, op := range h.ops {
		if op.isGroup {
			b = b.WithGroup(op.group)
		} else {
			b = b.WithAttrs(op.attrs)
		}
	}
	return b
}

func (h *invocationLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.resolveBase().Enabled(ctx, level)
}

func (h *invocationLogHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.effective(ctx).Handle(ctx, r)
}

func (h *invocationLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	cp := *h
	cp.ops = make([]logOp, 0, len(h.ops)+1)
	cp.ops = append(cp.ops, h.ops...)
	cp.ops = append(cp.ops, logOp{attrs: attrs})
	return &cp
}

func (h *invocationLogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		// slog's documented convention: empty WithGroup is a no-op.
		return h
	}
	cp := *h
	cp.ops = make([]logOp, 0, len(h.ops)+1)
	cp.ops = append(cp.ops, h.ops...)
	cp.ops = append(cp.ops, logOp{isGroup: true, group: name})
	return &cp
}
