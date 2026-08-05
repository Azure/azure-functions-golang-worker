package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/azure/azure-functions-golang-worker/internal/hostclient"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"

	"google.golang.org/grpc"
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
	return connectToHostWith(hostAddress, maxMsgSize, workerId, hostclient.OpenEventStream)
}

type hostStreamOpener func(string, int) (grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage], error)

func connectToHostWith(hostAddress string, maxMsgSize int, workerId string, open hostStreamOpener) (
	grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage], error) {
	client, err := open(hostAddress, maxMsgSize)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC stream: %w", err)
	}

	if err := sendStartStreamMessage(client, workerId); err != nil {
		return nil, fmt.Errorf("failed to send start stream message: %w", err)
	}

	return client, nil
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
