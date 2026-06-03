package sdk

import (
	"context"
	"errors"
	"testing"
)

// fakeHook is a minimal LifecycleHook for exercising StartOption wiring.
type fakeHook struct {
	startErr    error
	shutdownErr error
	started     bool
	shutdown    bool
}

func (h *fakeHook) Start(context.Context) error {
	h.started = true
	return h.startErr
}

func (h *fakeHook) Shutdown(context.Context) error {
	h.shutdown = true
	return h.shutdownErr
}

func TestWithLifecycleHook_AppendsInOrder(t *testing.T) {
	h1 := &fakeHook{}
	h2 := &fakeHook{}

	var cfg StartConfig
	for _, opt := range []StartOption{WithLifecycleHook(h1), WithLifecycleHook(h2)} {
		opt(&cfg)
	}

	if len(cfg.Hooks) != 2 {
		t.Fatalf("expected 2 hooks, got %d", len(cfg.Hooks))
	}
	if cfg.Hooks[0] != h1 || cfg.Hooks[1] != h2 {
		t.Errorf("hooks not registered in order")
	}
}

func TestWithLifecycleHook_NilIgnored(t *testing.T) {
	// Nil hooks must be dropped so callers can register conditional hooks
	// without an explicit guard.
	var cfg StartConfig
	WithLifecycleHook(nil)(&cfg)

	if len(cfg.Hooks) != 0 {
		t.Errorf("expected nil hook to be ignored, got %d hook(s)", len(cfg.Hooks))
	}
}

func TestStartConfig_HookStartShutdown(t *testing.T) {
	wantErr := errors.New("boom")
	h := &fakeHook{shutdownErr: wantErr}

	var cfg StartConfig
	WithLifecycleHook(h)(&cfg)

	if err := cfg.Hooks[0].Start(context.Background()); err != nil {
		t.Fatalf("unexpected Start error: %v", err)
	}
	if !h.started {
		t.Errorf("Start was not invoked")
	}
	if err := cfg.Hooks[0].Shutdown(context.Background()); err != wantErr {
		t.Errorf("expected shutdown error %v, got %v", wantErr, err)
	}
	if !h.shutdown {
		t.Errorf("Shutdown was not invoked")
	}
}
