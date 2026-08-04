package hostclient

import (
	"context"
	"fmt"
	"time"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// connectionTimeout stays below the Functions host's 60-second
// ProcessStartupTimeout so readiness failures can be logged before the host
// terminates the worker. The remaining budget covers EventStream setup and the
// StartStream handshake.
const connectionTimeout = 50 * time.Second

var connectParams = grpc.ConnectParams{
	Backoff: backoff.Config{
		BaseDelay:  100 * time.Millisecond,
		Multiplier: 1.6,
		Jitter:     0.2,
		MaxDelay:   500 * time.Millisecond,
	},
	MinConnectTimeout: time.Second,
}

// OpenEventStream connects to the local Functions host and opens its RPC event
// stream. The insecure transport preserves the existing worker-host protocol.
func OpenEventStream(address string, maxMessageSize int) (grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage], error) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageSize),
			grpc.MaxCallSendMsgSize(maxMessageSize),
		),
		grpc.WithConnectParams(connectParams),
	)
	if err != nil {
		return nil, fmt.Errorf("create gRPC client: %w", err)
	}

	return openEventStream(conn, connectionTimeout)
}

func openEventStream(conn hostConnection, timeout time.Duration) (grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage], error) {
	if err := waitForReady(conn, timeout); err != nil {
		_ = conn.Close()
		return nil, err
	}

	stream, err := pb.NewFunctionRpcClient(conn).EventStream(context.Background())
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open gRPC EventStream: %w", err)
	}
	return stream, nil
}

func waitForReady(conn readyConnection, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn.Connect()
	for {
		state := conn.GetState()
		switch state {
		case connectivity.Ready:
			return nil
		case connectivity.Shutdown:
			// A freshly created connection should not reach Shutdown on the
			// current call path, but it is terminal for any future callers.
			return fmt.Errorf("gRPC connection is in terminal state %s and cannot become ready", state)
		}

		if !conn.WaitForStateChange(ctx, state) {
			return fmt.Errorf(
				"timed out after %s waiting for gRPC connection to become ready (last state: %s): %w",
				timeout,
				state,
				ctx.Err(),
			)
		}
	}
}

// readyConnection is the subset of grpc.ClientConn used by waitForReady to
// activate the lazy connection and observe connectivity state transitions.
type readyConnection interface {
	Connect()
	GetState() connectivity.State
	WaitForStateChange(context.Context, connectivity.State) bool
}

// hostConnection extends readyConnection with the operations openEventStream
// needs to create the RPC stream and close the connection when setup fails.
type hostConnection interface {
	readyConnection
	grpc.ClientConnInterface
	Close() error
}
