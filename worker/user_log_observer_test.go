package worker

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// Each test in this file mutates the package-global userLogObservers
// slice. Running them in parallel with anything else that registers an
// observer would cross-contaminate. We snapshot and restore the slice
// around each test so other tests in the package (which never register
// an observer) are unaffected.
func withClearedUserLogObservers(t *testing.T) {
	t.Helper()
	prev := userLogObservers.Load()
	t.Cleanup(func() {
		userLogObservers.Store(prev)
	})
	userLogObservers.Store(nil)
}

// TestRegisterUserLogObserver_FansOut asserts the basic contract: every
// observer registered before a Handle call sees every record, and each
// observer sees the bound attrs the handler accumulated via WithAttrs.
func TestRegisterUserLogObserver_FansOut(t *testing.T) {
	withClearedUserLogObservers(t)

	type seen struct {
		msg   string
		attrs map[string]any
	}
	var mu sync.Mutex
	collect := func(label string) (UserLogObserver, *[]seen) {
		records := &[]seen{}
		fn := func(_ context.Context, r slog.Record) {
			mu.Lock()
			defer mu.Unlock()
			attrs := map[string]any{}
			r.Attrs(func(a slog.Attr) bool {
				attrs[a.Key] = a.Value.Any()
				return true
			})
			*records = append(*records, seen{msg: r.Message, attrs: attrs})
			_ = label
		}
		return fn, records
	}

	obs1, recs1 := collect("obs1")
	obs2, recs2 := collect("obs2")
	RegisterUserLogObserver(obs1)
	RegisterUserLogObserver(obs2)

	// Build a userLogHandler with one bound attr so we exercise the
	// observer-side attr propagation (the handler clones the record and
	// re-AddAttrs the bound attrs).
	h := (&userLogHandler{
		writer:   newLogWriter(func(*pb.StreamingMessage) error { return nil }, slog.NewTextHandler(discardWriter{}, nil)),
		composer: logComposer{}.withAttrs([]slog.Attr{slog.String("bound_key", "bound_val")}),
	})

	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "hello", 0)
	rec.AddAttrs(slog.String("inline_key", "inline_val"))
	if err := h.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	for _, r := range []*[]seen{recs1, recs2} {
		mu.Lock()
		got := *r
		mu.Unlock()
		if len(got) != 1 {
			t.Errorf("observer received %d records; want 1", len(got))
			continue
		}
		if got[0].msg != "hello" {
			t.Errorf("msg = %q, want %q", got[0].msg, "hello")
		}
		if got[0].attrs["inline_key"] != "inline_val" {
			t.Errorf("missing inline attr; got %+v", got[0].attrs)
		}
		if got[0].attrs["bound_key"] != "bound_val" {
			t.Errorf("missing bound attr; got %+v", got[0].attrs)
		}
	}
}

// TestRegisterUserLogObserver_NilIgnored verifies a nil function passed
// to RegisterUserLogObserver is silently dropped (so callers can wire
// conditional observers without an explicit nil guard).
func TestRegisterUserLogObserver_NilIgnored(t *testing.T) {
	withClearedUserLogObservers(t)
	RegisterUserLogObserver(nil)
	if obs := userLogObservers.Load(); obs != nil && len(*obs) != 0 {
		t.Errorf("nil observer must not be registered; got len=%d", len(*obs))
	}
}

// TestRegisterUserLogObserver_NoObserversNoOp confirms the user log
// handler does not allocate or invoke observer logic when none have been
// registered. The contract that "users who never import otelfunc pay no
// runtime cost" relies on this fast-path.
func TestRegisterUserLogObserver_NoObserversNoOp(t *testing.T) {
	withClearedUserLogObservers(t)

	h := &userLogHandler{
		writer: newLogWriter(func(*pb.StreamingMessage) error { return nil }, slog.NewTextHandler(discardWriter{}, nil)),
	}
	rec := slog.NewRecord(time.Now(), slog.LevelInfo, "hello", 0)
	if err := h.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	// Nothing to assert about the observer side -- the test is here to
	// catch any future refactor that introduces panics or nil-pointer
	// dereferences on the zero-observer path.
}

// discardWriter is a no-op io.Writer used as the stderr fallback when
// constructing LogWriter in these tests.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
