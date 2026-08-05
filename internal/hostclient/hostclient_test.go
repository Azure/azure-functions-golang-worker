package hostclient

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type fakeConnection struct {
	mu            sync.Mutex
	state         connectivity.State
	changeCh      chan struct{}
	connectCalled atomic.Bool
	closeCalled   atomic.Bool
	newStreamErr  error
}

func newFakeConnection(state connectivity.State) *fakeConnection {
	return &fakeConnection{
		state:    state,
		changeCh: make(chan struct{}, 1),
	}
}

func (f *fakeConnection) Connect() {
	f.connectCalled.Store(true)
}

func (f *fakeConnection) GetState() connectivity.State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state
}

func (f *fakeConnection) WaitForStateChange(ctx context.Context, sourceState connectivity.State) bool {
	for {
		if f.GetState() != sourceState {
			return true
		}
		select {
		case <-f.changeCh:
		case <-ctx.Done():
			return false
		}
	}
}

func (f *fakeConnection) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return nil
}

func (f *fakeConnection) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, f.newStreamErr
}

func (f *fakeConnection) Close() error {
	f.closeCalled.Store(true)
	return nil
}

func (f *fakeConnection) pushState(state connectivity.State) {
	f.mu.Lock()
	f.state = state
	f.mu.Unlock()
	select {
	case f.changeCh <- struct{}{}:
	default:
	}
}

func TestConnectionPolicy(t *testing.T) {
	if connectionTimeout != 50*time.Second {
		t.Fatalf("connectionTimeout = %s, want 50s", connectionTimeout)
	}
	if connectParams.Backoff.BaseDelay != 100*time.Millisecond {
		t.Fatalf("BaseDelay = %s, want 100ms", connectParams.Backoff.BaseDelay)
	}
	if connectParams.Backoff.Multiplier != 1.6 {
		t.Fatalf("Multiplier = %v, want 1.6", connectParams.Backoff.Multiplier)
	}
	if connectParams.Backoff.Jitter != 0.2 {
		t.Fatalf("Jitter = %v, want 0.2", connectParams.Backoff.Jitter)
	}
	if connectParams.Backoff.MaxDelay != 500*time.Millisecond {
		t.Fatalf("MaxDelay = %s, want 500ms", connectParams.Backoff.MaxDelay)
	}
	if connectParams.MinConnectTimeout != time.Second {
		t.Fatalf("MinConnectTimeout = %s, want 1s", connectParams.MinConnectTimeout)
	}
}

func TestWaitForReady_ConnectsAndObservesStateChanges(t *testing.T) {
	conn := newFakeConnection(connectivity.Idle)
	go func() {
		conn.pushState(connectivity.Connecting)
		conn.pushState(connectivity.Ready)
	}()

	if err := waitForReady(conn, time.Second); err != nil {
		t.Fatalf("waitForReady() error = %v", err)
	}
	if !conn.connectCalled.Load() {
		t.Fatal("waitForReady() did not call Connect()")
	}
}

func TestWaitForReady_ReturnsImmediatelyWhenReady(t *testing.T) {
	conn := newFakeConnection(connectivity.Ready)

	if err := waitForReady(conn, time.Second); err != nil {
		t.Fatalf("waitForReady() error = %v", err)
	}
	if !conn.connectCalled.Load() {
		t.Fatal("waitForReady() did not call Connect()")
	}
}

func TestWaitForReady_TimesOutWithLastStateAndWrappedDeadline(t *testing.T) {
	conn := newFakeConnection(connectivity.TransientFailure)

	err := waitForReady(conn, 20*time.Millisecond)

	if err == nil {
		t.Fatal("waitForReady() error = nil, want timeout")
	}
	if !strings.Contains(err.Error(), connectivity.TransientFailure.String()) {
		t.Fatalf("error %q does not include last state", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error %q does not wrap context.DeadlineExceeded", err)
	}
}

func TestWaitForReady_FailsFastOnShutdown(t *testing.T) {
	conn := newFakeConnection(connectivity.Shutdown)

	err := waitForReady(conn, time.Second)

	if err == nil {
		t.Fatal("waitForReady() error = nil, want terminal state error")
	}
	if !strings.Contains(err.Error(), connectivity.Shutdown.String()) {
		t.Fatalf("error %q does not include Shutdown", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error wraps context.DeadlineExceeded: %v", err)
	}
}

func TestOpenEventStream_ClosesConnectionWhenReadinessFails(t *testing.T) {
	conn := newFakeConnection(connectivity.Shutdown)

	_, err := openEventStream(conn, time.Second)

	if err == nil {
		t.Fatal("openEventStream() error = nil, want readiness failure")
	}
	if !conn.closeCalled.Load() {
		t.Fatal("openEventStream() did not close connection after readiness failure")
	}
}

func TestOpenEventStream_ClosesConnectionWhenStreamCreationFails(t *testing.T) {
	streamErr := errors.New("stream creation failed")
	conn := newFakeConnection(connectivity.Ready)
	conn.newStreamErr = streamErr

	_, err := openEventStream(conn, time.Second)

	if !errors.Is(err, streamErr) {
		t.Fatalf("openEventStream() error = %v, want %v", err, streamErr)
	}
	if !conn.closeCalled.Load() {
		t.Fatal("openEventStream() did not close connection after stream creation failure")
	}
}
