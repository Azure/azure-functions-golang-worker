package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// recordedSender captures messages pushed by the LogWriter for assertions
// in tests.
type recordedSender struct {
	mu   sync.Mutex
	msgs []*pb.StreamingMessage
	err  error
}

func (s *recordedSender) send(m *pb.StreamingMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = append(s.msgs, m)
	return s.err
}

func (s *recordedSender) records() []*pb.RpcLog {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*pb.RpcLog, 0, len(s.msgs))
	for _, m := range s.msgs {
		if rl := m.GetRpcLog(); rl != nil {
			out = append(out, rl)
		}
	}
	return out
}

func TestLogWriter_Write_EmitsRpcLogStreamingMessage(t *testing.T) {
	r := &recordedSender{}
	w := newLogWriter(r.send, nil)

	w.Write(&pb.RpcLog{
		Level:       pb.RpcLog_Information,
		Message:     "hello",
		LogCategory: pb.RpcLog_User,
	})

	got := r.records()
	if len(got) != 1 {
		t.Fatalf("expected 1 RpcLog message; got %d", len(got))
	}
	if got[0].Message != "hello" {
		t.Errorf("Message = %q, want %q", got[0].Message, "hello")
	}
	if got[0].LogCategory != pb.RpcLog_User {
		t.Errorf("LogCategory = %v, want User", got[0].LogCategory)
	}
}

func TestLogWriter_Write_NilDropped(t *testing.T) {
	r := &recordedSender{}
	w := newLogWriter(r.send, nil)
	w.Write(nil)
	if got := r.records(); len(got) != 0 {
		t.Errorf("nil RpcLog must be silently dropped; got %d records", len(got))
	}
}

func TestLogWriter_FilterByCategoryThreshold(t *testing.T) {
	r := &recordedSender{}
	w := newLogWriter(r.send, nil)

	// The host instructs the worker to suppress everything from
	// Function.Quiet but allow Worker traffic at Information+.
	w.SetCategories(map[string]pb.RpcLog_Level{
		"Worker":          pb.RpcLog_Information,
		"Function.Quiet":  pb.RpcLog_None,
		"Function.Loud.X": pb.RpcLog_Trace,
	})

	cases := []struct {
		category string
		level    pb.RpcLog_Level
		wantSent bool
		desc     string
	}{
		{"Worker", pb.RpcLog_Debug, false, "Worker debug below Information threshold"},
		{"Worker", pb.RpcLog_Information, true, "Worker info at threshold"},
		{"Function.Quiet", pb.RpcLog_Critical, false, "Quiet category suppressed entirely"},
		{"Function.Loud.X", pb.RpcLog_Trace, true, "Loud.X allowed at Trace"},
		{"Function.Loud.X.Sub", pb.RpcLog_Debug, true, "Sub-category inherits Loud.X threshold"},
		{"Custom.Unknown", pb.RpcLog_Information, true, "Falls back to Worker default"},
		{"Custom.Unknown", pb.RpcLog_Debug, false, "Below Worker default"},
	}
	for _, c := range cases {
		// Reset between cases so we can isolate each.
		r.mu.Lock()
		r.msgs = nil
		r.mu.Unlock()

		w.Write(&pb.RpcLog{Category: c.category, Level: c.level, Message: c.desc})
		got := len(r.records()) == 1
		if got != c.wantSent {
			t.Errorf("%s: sent=%v want=%v", c.desc, got, c.wantSent)
		}
	}
}

func TestLogWriter_NoCategoriesAllowsAllLevels(t *testing.T) {
	// The current Functions host does not populate
	// WorkerInitRequest.LogCategories, so most workers run with no
	// host-supplied filter at all. In that case we must forward every
	// record (including Trace and Debug) so host.json's
	// logging.logLevel.* configuration can decide what surfaces -- the
	// host is the canonical place to filter by level.
	r := &recordedSender{}
	w := newLogWriter(r.send, nil)

	for _, lvl := range []pb.RpcLog_Level{
		pb.RpcLog_Trace,
		pb.RpcLog_Debug,
		pb.RpcLog_Information,
		pb.RpcLog_Warning,
		pb.RpcLog_Error,
		pb.RpcLog_Critical,
	} {
		w.Write(&pb.RpcLog{Category: "Function.Anything", Level: lvl, Message: lvl.String()})
	}
	if got := len(r.records()); got != 6 {
		t.Errorf("expected all 6 levels forwarded with no categories installed; got %d", got)
	}
}

func TestLogWriter_FallbackOnSendError(t *testing.T) {
	failingSender := &recordedSender{err: errors.New("stream closed")}
	var fb buffHandler
	w := newLogWriter(failingSender.send, &fb)

	w.Write(&pb.RpcLog{Level: pb.RpcLog_Error, Message: "boom"})

	if got := fb.count(); got != 1 {
		t.Errorf("expected fallback handler to receive 1 record on send error; got %d", got)
	}
}

// buffHandler is a tiny stub slog.Handler counting Handle calls. Used by
// the fallback test above.
type buffHandler struct {
	mu  sync.Mutex
	cnt int
}

func (h *buffHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cnt
}
func (h *buffHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *buffHandler) Handle(_ context.Context, _ slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cnt++
	return nil
}
func (h *buffHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *buffHandler) WithGroup(_ string) slog.Handler      { return h }

func TestUserLogHandler_AttachesInvocationFromContext(t *testing.T) {
	r := &recordedSender{}
	w := newLogWriter(r.send, nil)
	logger := slog.New(newUserLogHandler(w))

	ic := &sdk.InvocationContext{
		InvocationID: "inv-99",
		FunctionName: "Greeter",
	}
	ctx := sdk.NewContext(context.Background(), ic)
	logger.InfoContext(ctx, "ping", "k", "v")

	got := r.records()
	if len(got) != 1 {
		t.Fatalf("expected 1 record; got %d", len(got))
	}
	if got[0].InvocationId != "inv-99" {
		t.Errorf("InvocationId = %q, want inv-99", got[0].InvocationId)
	}
	if got[0].Category != "Function.Greeter" {
		t.Errorf("Category = %q, want Function.Greeter", got[0].Category)
	}
	if got[0].LogCategory != pb.RpcLog_User {
		t.Errorf("LogCategory = %v, want User", got[0].LogCategory)
	}
}

func TestSystemLogHandler_DefaultsToWorkerCategory(t *testing.T) {
	r := &recordedSender{}
	w := newLogWriter(r.send, nil)
	logger := slog.New(newSystemLogHandler(w))

	logger.Info("starting up")

	got := r.records()
	if len(got) != 1 {
		t.Fatalf("expected 1 record; got %d", len(got))
	}
	if got[0].Category != "Worker" {
		t.Errorf("default Category = %q, want Worker", got[0].Category)
	}
	if got[0].LogCategory != pb.RpcLog_System {
		t.Errorf("LogCategory = %v, want System", got[0].LogCategory)
	}
}

func TestSlogLevelToRpc(t *testing.T) {
	cases := []struct {
		in   slog.Level
		want pb.RpcLog_Level
	}{
		{slog.LevelDebug - 4, pb.RpcLog_Trace},
		{slog.LevelDebug, pb.RpcLog_Debug},
		{slog.LevelInfo, pb.RpcLog_Information},
		{slog.LevelWarn, pb.RpcLog_Warning},
		{slog.LevelError, pb.RpcLog_Error},
		{slog.LevelError + 4, pb.RpcLog_Critical},
	}
	for _, c := range cases {
		if got := slogLevelToRpc(c.in); got != c.want {
			t.Errorf("slogLevelToRpc(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}
