package worker

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// streamSender is the abstraction the LogWriter uses to push StreamingMessage
// values out to the host. Production code passes a function that pushes onto
// the dispatcher's outbound channel; tests pass a stub that records messages.
type streamSender func(*pb.StreamingMessage) error

// LogWriter is the worker-side component that translates already-built
// RpcLog values into StreamingMessage payloads on the bidirectional gRPC
// stream to the Functions host.
//
// The host filters logs by category at the worker side via WorkerInitRequest's
// log_categories map: e.g. "Worker = Verbose, Function.MyFunc = None"
// suppresses all logs from MyFunc but lets the worker's own logs through.
// LogWriter applies that filter before sending.
//
// LogWriter is safe for concurrent use; the supplied send function must be
// goroutine-safe (the dispatcher's channel-based outbound sender is).
type LogWriter struct {
	send streamSender

	mu         sync.RWMutex
	categories map[string]pb.RpcLog_Level

	// stderrFallback receives records when the send function returns an
	// error (e.g. the gRPC stream closed). Production code points this at
	// the bootstrap-style handler so logs are never silently dropped.
	stderrFallback slog.Handler
}

// newLogWriter constructs a LogWriter. send must be a goroutine-safe push
// onto the dispatcher's outbound channel. The optional stderrFallback is
// consulted when the send function returns an error; pass nil to suppress
// fallback writes (typically only desirable in tests).
func newLogWriter(send streamSender, stderrFallback slog.Handler) *LogWriter {
	return &LogWriter{
		send:           send,
		stderrFallback: stderrFallback,
	}
}

// SetCategories installs the host's log-category → minimum-level filter,
// typically populated from WorkerInitRequest.LogCategories.
//
// Categories are matched longest-prefix-first: a record categorized
// "Function.MyFunc.User" is checked against the longest matching key in
// the filter map (e.g. "Function.MyFunc" beats "Function" beats "Worker").
// The default minimum level when no entry matches is Information,
// mirroring the host's default category.
func (w *LogWriter) SetCategories(c map[string]pb.RpcLog_Level) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if c == nil {
		w.categories = nil
		return
	}
	cp := make(map[string]pb.RpcLog_Level, len(c))
	for k, v := range c {
		cp[k] = v
	}
	w.categories = cp
}

// Write enqueues an RpcLog onto the gRPC stream after applying the
// host-supplied category filter. Records below the filter threshold are
// dropped silently. Send errors fall back to the stderr handler so logs
// are not lost when the gRPC stream is closing.
func (w *LogWriter) Write(rl *pb.RpcLog) {
	if rl == nil {
		return
	}
	if !w.allowed(rl) {
		return
	}
	msg := &pb.StreamingMessage{
		Content: &pb.StreamingMessage_RpcLog{RpcLog: rl},
	}
	if err := w.send(msg); err != nil {
		w.fallback(rl, err)
	}
}

// allowed reports whether the host-supplied category filter permits this
// record. A record is permitted when its level is at or above the
// threshold for the most specific matching category.
//
// When no categories have been installed (the default for the current
// host, which does not populate WorkerInitRequest.LogCategories) every
// record is forwarded. This matches the .NET worker behavior: emit
// everything and let the host filter on the receiving side using
// host.json's logging.logLevel configuration. Without this default-allow
// the worker would drop Debug records before the host ever sees them,
// rendering host.json log-level overrides ineffective.
func (w *LogWriter) allowed(rl *pb.RpcLog) bool {
	w.mu.RLock()
	cats := w.categories
	w.mu.RUnlock()

	if len(cats) == 0 {
		return true
	}

	threshold := matchCategoryThreshold(cats, rl.GetCategory())
	return rl.GetLevel() >= threshold
}

// fallback forwards a record to the stderr fallback handler when the gRPC
// send function returned an error. Best effort: if the fallback also fails
// the record is dropped.
func (w *LogWriter) fallback(rl *pb.RpcLog, sendErr error) {
	if w.stderrFallback == nil {
		return
	}
	level := rpcLevelToSlog(rl.GetLevel())
	r := slog.NewRecord(time.Now(), level,
		fmt.Sprintf("%s [send-failed: %v]", rl.GetMessage(), sendErr), 0)
	if cat := rl.GetCategory(); cat != "" {
		r.AddAttrs(slog.String("category", cat))
	}
	if invID := rl.GetInvocationId(); invID != "" {
		r.AddAttrs(slog.String("invocation_id", invID))
	}
	_ = w.stderrFallback.Handle(context.Background(), r)
}

// matchCategoryThreshold finds the minimum level that should permit a
// record in the given category. Falls back to RpcLog_Information when
// nothing matches — the host's documented default category level.
//
// Matching rules, applied in order:
//
//  1. Exact match on category wins.
//  2. Otherwise, longest dotted-prefix match wins. "Function.MyFunc"
//     matches "Function.MyFunc.User" but "Func" does not match "Function"
//     (we require a "." boundary).
//  3. The catch-all keys "Worker" and "" (empty string) act as the
//     default for any record that does not match.
func matchCategoryThreshold(cats map[string]pb.RpcLog_Level, category string) pb.RpcLog_Level {
	if len(cats) == 0 {
		return pb.RpcLog_Information
	}
	if v, ok := cats[category]; ok {
		return v
	}

	bestKey := ""
	bestLevel := pb.RpcLog_Information
	matched := false
	for k, v := range cats {
		if k == "" || k == "Worker" || k == category {
			continue
		}
		if !strings.HasPrefix(category, k+".") {
			continue
		}
		if !matched || len(k) > len(bestKey) {
			bestKey = k
			bestLevel = v
			matched = true
		}
	}
	if matched {
		return bestLevel
	}

	if v, ok := cats["Worker"]; ok {
		return v
	}
	if v, ok := cats[""]; ok {
		return v
	}
	return pb.RpcLog_Information
}

// rpcLevelToSlog converts an RpcLog_Level to the closest slog.Level for
// fallback emission.
func rpcLevelToSlog(l pb.RpcLog_Level) slog.Level {
	switch l {
	case pb.RpcLog_Trace, pb.RpcLog_Debug:
		return slog.LevelDebug
	case pb.RpcLog_Information:
		return slog.LevelInfo
	case pb.RpcLog_Warning:
		return slog.LevelWarn
	case pb.RpcLog_Error, pb.RpcLog_Critical:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// slogLevelToRpc converts an slog.Level into the closest RpcLog_Level for
// emission. Used by both the user and system slog handlers.
func slogLevelToRpc(l slog.Level) pb.RpcLog_Level {
	switch {
	case l <= slog.LevelDebug-4:
		return pb.RpcLog_Trace
	case l <= slog.LevelDebug:
		return pb.RpcLog_Debug
	case l <= slog.LevelInfo:
		return pb.RpcLog_Information
	case l <= slog.LevelWarn:
		return pb.RpcLog_Warning
	case l <= slog.LevelError:
		return pb.RpcLog_Error
	default:
		return pb.RpcLog_Critical
	}
}
