package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
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
