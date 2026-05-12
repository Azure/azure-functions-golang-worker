package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// outboundQueueSize is the buffered size of the channel used by the gRPC
// sender goroutine. Sized for typical Functions workloads where a single
// invocation might emit a handful of logs plus the response message; the
// host drains messages quickly enough that backpressure is rare.
const outboundQueueSize = 256

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

	client := pb.NewFunctionRpcClient(conn)
	return client.EventStream(context.Background())
}

// sendStartStreamMessage establishes the worker -> host handshake. After
// it returns, the host sends WorkerInitRequest.
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

// startSender owns the gRPC ClientStream's Send call. gRPC's Send is not
// safe for concurrent use, so we serialize all writes through a single
// goroutine that drains a buffered channel.
//
// Returns:
//   - send: a goroutine-safe enqueue function. Pass it to LogWriter and
//     anywhere a response message needs to be emitted.
//   - stop: shuts the sender goroutine down. Safe to call any number of
//     times concurrently; only the first call closes the queue.
//   - done: closed when the sender goroutine has exited (either because
//     stop was called or because Send returned an error). Callers may
//     wait on it to know the gRPC stream is no longer usable.
//
// When client.Send returns an error the sender logs it through the
// supplied bootstrap handler (so we don't deadlock by recursing through
// the LogWriter) and exits. Subsequent enqueues become no-ops returning
// the captured error.
func startSender(client grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage], errLogger *slog.Logger) (send streamSender, stop func(), done <-chan struct{}) {
	queue := make(chan *pb.StreamingMessage, outboundQueueSize)
	finished := make(chan struct{})

	go func() {
		defer close(finished)
		for msg := range queue {
			if err := client.Send(msg); err != nil {
				errLogger.LogAttrs(context.Background(), slog.LevelError, "gRPC send failed; sender exiting",
					slog.Any("err", err),
				)
				// Drain remaining messages to unblock producers that
				// are mid-send on the buffered channel. Once we return
				// and the deferred close(finished) fires, subsequent
				// producers exit via the <-finished arm of the select
				// in sendFn and get io.ErrClosedPipe.
				for range queue {
				}
				return
			}
		}
	}()

	// stopOnce makes stopFn safe for concurrent and repeated calls; only
	// the first invocation closes the queue. Without this the bool flag
	// would race between two callers (e.g. signal handler and Start's
	// normal-exit path) and produce a "close of closed channel" panic.
	var stopOnce sync.Once
	stopFn := func() {
		stopOnce.Do(func() {
			close(queue)
		})
	}

	sendFn := func(m *pb.StreamingMessage) error {
		select {
		case queue <- m:
			return nil
		case <-finished:
			return io.ErrClosedPipe
		}
	}

	return sendFn, stopFn, finished
}
