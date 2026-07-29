package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// outboundQueueSize is the capacity, in messages, of the buffered channel
// feeding the gRPC sender goroutine. Each slot holds one *pb.StreamingMessage
// (a response or a log line). Sized generously; the host drains fast enough
// that backpressure is rare.
const outboundQueueSize = 256

// streamSender enqueues a message onto the worker's outbound gRPC queue.
// Safe for concurrent use. Its signature matches what [log.NewWriter] expects.
type streamSender func(*pb.StreamingMessage) error

func connectToHost(hostAddress string, maxMsgSize int, workerId string) (
	grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage], error) {
	client, err := getBidiStreamClient(hostAddress, maxMsgSize)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC stream: %v", err)
	}

	if err := sendStartStreamMessage(client, workerId); err != nil {
		return nil, fmt.Errorf("failed to send start stream message: %v", err)
	}

	return client, nil
}

func getBidiStreamClient(address string, maxMsgSize int) (grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage], error) {
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize), grpc.MaxCallSendMsgSize(maxMsgSize)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return nil, err
	}

	// Drive the connection to READY before opening the stream. Otherwise
	// EventStream would fast-fail if the host isn't listening yet, which
	// happens when the host launches the worker as part of its startup.
	if err := waitForConnReady(conn, connectTimeout); err != nil {
		_ = conn.Close()
		return nil, err
	}

	client := pb.NewFunctionRpcClient(conn)
	return client.EventStream(context.Background())
}

// connectTimeout bounds the initial wait for the host's gRPC server. If it
// isn't listening within this window, fail loudly instead of hanging.
const connectTimeout = 5 * time.Second

// waitForConnReady blocks until conn reaches connectivity.Ready or the
// timeout expires. gRPC handles dial retries and backoff internally.
func waitForConnReady(conn readyConn, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// grpc.NewClient is lazy: the connection stays in IDLE until the first
	// RPC. Without this call the loop below has nothing to observe and
	// every startup would hang for the full connectTimeout.
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		// Shutdown is terminal — the channel can never become Ready again,
		// so waiting for the full timeout would only add startup latency
		// and hide the real failure. Idle/Connecting/TransientFailure are
		// all recoverable; gRPC drives its own retries and backoff between
		// them, so we just keep observing.
		if state == connectivity.Shutdown {
			return fmt.Errorf("gRPC connection is in terminal state %s and cannot become ready", state)
		}
		if !conn.WaitForStateChange(ctx, state) {
			return fmt.Errorf("timed out after %s waiting for gRPC connection to become ready (last state: %s): %w",
				timeout, state, ctx.Err())
		}
	}
}

// readyConn is the subset of *grpc.ClientConn used by waitForConnReady,
// carved out so tests can supply a fake.
type readyConn interface {
	Connect()
	GetState() connectivity.State
	WaitForStateChange(ctx context.Context, sourceState connectivity.State) bool
}

// sendStartStreamMessage performs the worker -> host handshake, after which
// the host sends WorkerInitRequest.
func sendStartStreamMessage(client grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage], workerId string) error {
	startStreamMsg := &pb.StreamingMessage{
		Content: &pb.StreamingMessage_StartStream{
			StartStream: &pb.StartStream{
				WorkerId: workerId,
			},
		},
	}
	return client.Send(startStreamMsg)
}

// startSender serializes writes to the gRPC stream. grpc.ClientStream.Send
// is not safe for concurrent use, so every message goes through a single
// goroutine draining a buffered channel.
//
// Returns:
//   - send: enqueues a message; safe for concurrent callers.
//   - stop: shuts the sender down. Idempotent.
//   - done: closed once the sender goroutine has exited (via stop or a
//     Send error). Signals that the gRPC stream is no longer usable.
//
// On Send failure the sender logs via errLogger — not the LogWriter, which
// would recurse back through this same sender — and exits. Later enqueues
// return io.ErrClosedPipe.
func startSender(client grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage], errLogger *slog.Logger) (send streamSender, stop func(), done <-chan struct{}) {
	queue := make(chan *pb.StreamingMessage, outboundQueueSize)
	finished := make(chan struct{})

	// stopChan is close-once so multiple sendFn callers can detect shutdown
	// without racing. closed is a fast-path check that avoids a channel op
	// on the hot enqueue path.
	stopChan := make(chan struct{})
	var stopOnce sync.Once
	var closed atomic.Bool

	go func() {
		defer close(finished)
		for {
			select {
			case <-stopChan:
				closed.Store(true)
				return
			case msg := <-queue:
				if msg == nil {
					closed.Store(true)
					return
				}
				if err := client.Send(msg); err != nil {
					errLogger.LogAttrs(context.Background(), slog.LevelError, "gRPC send failed; sender exiting",
						slog.Any("err", err),
					)
					closed.Store(true)
					return
				}
			}
		}
	}()

	stopFn := func() {
		stopOnce.Do(func() {
			closed.Store(true)
			close(stopChan)
		})
	}

	sendFn := func(m *pb.StreamingMessage) error {
		if closed.Load() {
			return io.ErrClosedPipe
		}
		select {
		case <-stopChan:
			return io.ErrClosedPipe
		default:
		}
		select {
		case <-stopChan:
			return io.ErrClosedPipe
		case queue <- m:
			return nil
		case <-finished:
			return io.ErrClosedPipe
		}
	}

	return sendFn, stopFn, finished
}
