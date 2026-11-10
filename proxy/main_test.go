package main

import (
	"context"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azure/azure-functions-golang-worker/worker"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type recordingHostStream struct {
	mu   sync.Mutex
	sent []*pb.StreamingMessage
}

func TestConnectToHostWith_UsesSharedStreamAndSendsProxyStartMessage(t *testing.T) {
	stream := &recordingHostStream{}
	config := &worker.WorkerStartupConfig{
		FunctionsUri:                  "host:1234",
		FunctionsWorkerId:             "worker-id",
		FunctionsRequestId:            "request-id",
		FunctionsGrpcMaxMessageLength: 1024,
	}
	proxy := &Proxy{config: config}
	open := func(address string, maxMessageSize int) (grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage], error) {
		if address != config.FunctionsUri {
			t.Fatalf("address = %q, want %q", address, config.FunctionsUri)
		}
		if maxMessageSize != config.FunctionsGrpcMaxMessageLength {
			t.Fatalf("maxMessageSize = %d, want %d", maxMessageSize, config.FunctionsGrpcMaxMessageLength)
		}
		return stream, nil
	}

	if err := proxy.connectToHostWith(open); err != nil {
		t.Fatalf("connectToHostWith() error = %v", err)
	}
	if proxy.hostStream != stream {
		t.Fatal("proxy hostStream was not set to the shared stream")
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(stream.sent))
	}
	msg := stream.sent[0]
	if msg.RequestId != config.FunctionsRequestId {
		t.Fatalf("request ID = %q, want %q", msg.RequestId, config.FunctionsRequestId)
	}
	if msg.GetStartStream().GetWorkerId() != config.FunctionsWorkerId {
		t.Fatalf("worker ID = %q, want %q", msg.GetStartStream().GetWorkerId(), config.FunctionsWorkerId)
	}
}

func TestStartLocalServer_ChildMessageLimitFollowsHostMaxMessageLength(t *testing.T) {
	const grpcDefaultLimit = 4 << 20

	tests := []struct {
		name      string
		limit     int
		size      int
		wantError bool
	}{
		{name: "above the grpc default when the host allows it", limit: math.MaxInt32, size: grpcDefaultLimit + 1<<20},
		{name: "above the announced limit", limit: 1 << 20, size: 2 << 20, wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Proxy{
				config:         &worker.WorkerStartupConfig{FunctionsGrpcMaxMessageLength: tt.limit},
				childConnected: make(chan struct{}),
			}
			if err := p.startLocalServer(); err != nil {
				t.Fatalf("startLocalServer() error = %v", err)
			}
			defer p.server.Stop()

			conn, err := grpc.NewClient(
				p.listener.Addr().String(),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(math.MaxInt32)),
			)
			if err != nil {
				t.Fatalf("grpc.NewClient() error = %v", err)
			}
			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			stream, err := pb.NewFunctionRpcClient(conn).EventStream(ctx)
			if err != nil {
				t.Fatalf("EventStream() error = %v", err)
			}
			select {
			case <-p.childConnected:
			case <-ctx.Done():
				t.Fatal("the child never connected to the proxy")
			}

			big := &pb.StreamingMessage{Content: &pb.StreamingMessage_InvocationResponse{
				InvocationResponse: &pb.InvocationResponse{
					InvocationId: "invocation-id",
					ReturnValue:  &pb.TypedData{Data: &pb.TypedData_Bytes{Bytes: make([]byte, tt.size)}},
				},
			}}
			go func() { _ = stream.Send(big) }()

			p.mutex.Lock()
			childStream := p.childStream
			p.mutex.Unlock()

			msg, err := childStream.Recv()
			if tt.wantError {
				if status.Code(err) != codes.ResourceExhausted {
					t.Fatalf("Recv() error = %v, want code %s", err, codes.ResourceExhausted)
				}
				return
			}
			if err != nil {
				t.Fatalf("the proxy rejected a %d byte message from the child: %v", tt.size, err)
			}
			if got := len(msg.GetInvocationResponse().GetReturnValue().GetBytes()); got != tt.size {
				t.Fatalf("received %d bytes, want %d", got, tt.size)
			}
		})
	}
}

func (s *recordingHostStream) Send(msg *pb.StreamingMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, msg)
	return nil
}

func (s *recordingHostStream) Recv() (*pb.StreamingMessage, error) {
	return nil, io.EOF
}

func (s *recordingHostStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (s *recordingHostStream) Trailer() metadata.MD {
	return nil
}

func (s *recordingHostStream) CloseSend() error {
	return nil
}

func (s *recordingHostStream) Context() context.Context {
	return context.Background()
}

func (s *recordingHostStream) SendMsg(any) error {
	return nil
}

func (s *recordingHostStream) RecvMsg(any) error {
	return io.EOF
}

func TestAppBinaryPath_Default(t *testing.T) {
	os.Unsetenv("FUNCTIONS_APP_BINARY_NAME")
	path := appBinaryPath("/home/site/wwwroot")
	expected := filepath.Join("/home/site/wwwroot", "app")
	if path != expected {
		t.Errorf("expected %s, got %s", expected, path)
	}
}

func TestAppBinaryPath_CustomName(t *testing.T) {
	os.Setenv("FUNCTIONS_APP_BINARY_NAME", "myservice")
	defer os.Unsetenv("FUNCTIONS_APP_BINARY_NAME")

	path := appBinaryPath("/home/site/wwwroot")
	expected := filepath.Join("/home/site/wwwroot", "myservice")
	if path != expected {
		t.Errorf("expected %s, got %s", expected, path)
	}
}

func TestAppBinaryPath_PicksUpSetenv(t *testing.T) {
	// Simulate FERR setting FUNCTIONS_APP_BINARY_NAME via os.Setenv
	os.Unsetenv("FUNCTIONS_APP_BINARY_NAME")

	// Before FERR: default name
	path := appBinaryPath("/app")
	expected := filepath.Join("/app", "app")
	if path != expected {
		t.Errorf("before setenv: expected %s, got %s", expected, path)
	}

	// Simulate FERR applying env vars
	os.Setenv("FUNCTIONS_APP_BINARY_NAME", "custom")
	defer os.Unsetenv("FUNCTIONS_APP_BINARY_NAME")

	// After FERR: custom name
	path = appBinaryPath("/app")
	expected = filepath.Join("/app", "custom")
	if path != expected {
		t.Errorf("after setenv: expected %s, got %s", expected, path)
	}
}

func TestSetenvOverridesExisting(t *testing.T) {
	os.Setenv("WEBSITE_PLACEHOLDER_MODE", "1")
	defer os.Unsetenv("WEBSITE_PLACEHOLDER_MODE")

	if os.Getenv("WEBSITE_PLACEHOLDER_MODE") != "1" {
		t.Fatal("expected 1")
	}

	// Simulate FERR override
	os.Setenv("WEBSITE_PLACEHOLDER_MODE", "0")

	if os.Getenv("WEBSITE_PLACEHOLDER_MODE") != "0" {
		t.Fatal("expected 0 after override")
	}

	// Verify os.Environ() doesn't have duplicates
	count := 0
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "WEBSITE_PLACEHOLDER_MODE=") {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 WEBSITE_PLACEHOLDER_MODE entry, got %d", count)
	}
}
