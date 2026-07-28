package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
)

type fakeBidiClient struct {
	mu      sync.Mutex
	sendErr error
	sendCnt int
}

func (f *fakeBidiClient) Send(_ *pb.StreamingMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sendCnt++
	return f.sendErr
}

func (f *fakeBidiClient) Recv() (*pb.StreamingMessage, error) {
	return nil, io.EOF
}

func (f *fakeBidiClient) Header() (metadata.MD, error) {
	return metadata.MD{}, nil
}

func (f *fakeBidiClient) Trailer() metadata.MD {
	return metadata.MD{}
}

func (f *fakeBidiClient) CloseSend() error {
	return nil
}

func (f *fakeBidiClient) Context() context.Context {
	return context.Background()
}

func (f *fakeBidiClient) SendMsg(any) error {
	return nil
}

func (f *fakeBidiClient) RecvMsg(any) error {
	return io.EOF
}

func TestStartSender_StopClosesDoneAndRejectsNewSends(t *testing.T) {
	client := &fakeBidiClient{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	send, stop, done := startSender(client, logger)

	if err := send(&pb.StreamingMessage{}); err != nil {
		t.Fatalf("initial send: %v", err)
	}

	stop()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sender did not stop after stop()")
	}

	if err := send(&pb.StreamingMessage{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("send after stop: got %v want %v", err, io.ErrClosedPipe)
	}
}

func TestStartSender_SendErrorClosesDoneAndRejectsFutureSends(t *testing.T) {
	client := &fakeBidiClient{sendErr: errors.New("boom")}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	send, _, done := startSender(client, logger)

	if err := send(&pb.StreamingMessage{}); err != nil {
		t.Fatalf("enqueue before send failure: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sender did not stop after gRPC send failure")
	}

	if err := send(&pb.StreamingMessage{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("send after sender failure: got %v want %v", err, io.ErrClosedPipe)
	}
}

// fakeReadyConn drives waitForConnReady in tests without needing a real gRPC
// client. States are consumed in order: each GetState() pop reflects the next
// scripted state, and WaitForStateChange blocks (respecting ctx) until either
// another state is pushed or the deadline expires. This models gRPC's own
// state-notification semantics closely enough for the retry-timing tests.
type fakeReadyConn struct {
	mu             sync.Mutex
	states         []connectivity.State
	changeCh       chan struct{}
	connectCalled  bool
}

func newFakeReadyConn(initial connectivity.State) *fakeReadyConn {
	return &fakeReadyConn{
		states:   []connectivity.State{initial},
		changeCh: make(chan struct{}, 16),
	}
}

func (f *fakeReadyConn) Connect() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connectCalled = true
}

func (f *fakeReadyConn) GetState() connectivity.State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.states[len(f.states)-1]
}

func (f *fakeReadyConn) WaitForStateChange(ctx context.Context, sourceState connectivity.State) bool {
	// Loop until the observed state differs from sourceState, or ctx expires.
	for {
		f.mu.Lock()
		current := f.states[len(f.states)-1]
		f.mu.Unlock()
		if current != sourceState {
			return true
		}
		select {
		case <-f.changeCh:
			// A new state was pushed; loop to re-read.
		case <-ctx.Done():
			return false
		}
	}
}

// pushState appends a new state and wakes anyone parked in WaitForStateChange.
func (f *fakeReadyConn) pushState(s connectivity.State) {
	f.mu.Lock()
	f.states = append(f.states, s)
	f.mu.Unlock()
	select {
	case f.changeCh <- struct{}{}:
	default:
	}
}

func TestWaitForConnReady_ReturnsWhenReady(t *testing.T) {
	conn := newFakeReadyConn(connectivity.Idle)

	// Simulate the host coming up shortly after the worker starts to dial:
	// IDLE -> CONNECTING -> READY.
	go func() {
		time.Sleep(20 * time.Millisecond)
		conn.pushState(connectivity.Connecting)
		time.Sleep(20 * time.Millisecond)
		conn.pushState(connectivity.Ready)
	}()

	start := time.Now()
	if err := waitForConnReady(conn, 2*time.Second); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("waited too long for ready state: %s", elapsed)
	}
	if !conn.connectCalled {
		t.Fatal("Connect() was not called to nudge the conn out of IDLE")
	}
}

func TestWaitForConnReady_TimesOutWhenNeverReady(t *testing.T) {
	// Conn parked in TRANSIENT_FAILURE and never recovers — models the host
	// being unreachable for the full timeout window.
	conn := newFakeReadyConn(connectivity.TransientFailure)

	start := time.Now()
	err := waitForConnReady(conn, 100*time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("error should mention timeout, got: %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error should wrap context.DeadlineExceeded, got: %v", err)
	}
	if elapsed < 100*time.Millisecond {
		t.Fatalf("returned before deadline elapsed: %s", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("returned well after deadline (leak?): %s", elapsed)
	}
}
