package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
)

// recordingHook is a LifecycleHook that records the order in which Start and
// Shutdown are invoked across a set of sibling hooks via a shared event log.
type recordingHook struct {
	name     string
	startErr error
	events   *[]string
}

func (h *recordingHook) Start(context.Context) error {
	*h.events = append(*h.events, "start:"+h.name)
	return h.startErr
}

func (h *recordingHook) Shutdown(context.Context) error {
	*h.events = append(*h.events, "shutdown:"+h.name)
	return nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestStartLifecycleHooks_AllSucceed(t *testing.T) {
	var events []string
	hooks := []sdk.LifecycleHook{
		&recordingHook{name: "a", events: &events},
		&recordingHook{name: "b", events: &events},
	}

	if err := startLifecycleHooks(context.Background(), hooks, discardLogger()); err != nil {
		t.Fatalf("startLifecycleHooks returned error: %v", err)
	}

	want := []string{"start:a", "start:b"}
	if !equalStrings(events, want) {
		t.Errorf("events = %v, want %v", events, want)
	}
}

func TestStartLifecycleHooks_UnwindsStartedHooksOnFailure(t *testing.T) {
	var events []string
	boom := errors.New("boom")
	hooks := []sdk.LifecycleHook{
		&recordingHook{name: "a", events: &events},
		&recordingHook{name: "b", events: &events},
		&recordingHook{name: "c", startErr: boom, events: &events},
		&recordingHook{name: "d", events: &events}, // must never start
	}

	err := startLifecycleHooks(context.Background(), hooks, discardLogger())
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom error, got %v", err)
	}

	// a and b started (in order); c failed to start; d never started. The
	// successfully started hooks unwind in reverse order (b then a). c and d
	// are never shut down because they never came up.
	want := []string{"start:a", "start:b", "start:c", "shutdown:b", "shutdown:a"}
	if !equalStrings(events, want) {
		t.Errorf("events = %v, want %v", events, want)
	}
}

func TestShutdownLifecycleHooks_ReverseOrder(t *testing.T) {
	var events []string
	hooks := []sdk.LifecycleHook{
		&recordingHook{name: "a", events: &events},
		&recordingHook{name: "b", events: &events},
		&recordingHook{name: "c", events: &events},
	}

	shutdownLifecycleHooks(context.Background(), hooks, discardLogger())

	want := []string{"shutdown:c", "shutdown:b", "shutdown:a"}
	if !equalStrings(events, want) {
		t.Errorf("events = %v, want %v", events, want)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
