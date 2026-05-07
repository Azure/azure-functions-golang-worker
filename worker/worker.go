package worker

import (
	"io"
	"log/slog"
	"os"

	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// Start initializes the worker and connects to the host. It is the single
// entry point user main() functions call.
//
// Logging lifecycle:
//
//  1. Before Start, the sdk package's init function has installed an slog
//     default whose underlying base is the bootstrap stderr handler with
//     the LanguageWorkerConsoleLog[ts] prefix. Any logs emitted during
//     argument parsing or gRPC dial errors land on stderr in that format,
//     which the host recognizes.
//  2. As soon as the gRPC stream is open, Start constructs a [LogWriter]
//     and registers it as the SDK's default base handler via
//     [sdk.SetDefaultBaseHandler]. From this point on, slog calls in user
//     code emit RpcLog values over the gRPC stream with category=User and
//     are filtered by the host's log_categories map.
//  3. A separate System-category slog.Logger is stashed on the dispatcher
//     and used for worker-internal events (function load, message
//     dispatch, errors). It bypasses the SDK wrapper so it doesn't pick
//     up invocation_id / function_name attrs from the user's
//     InvocationContext.
//
// Start blocks until the gRPC bidi stream closes or returns an error.
func Start(app *sdk.App) {
	bootstrap := slog.New(newBootstrapHandler(os.Stderr))

	config, err := GetWorkerStartupConfig()
	if err != nil {
		bootstrap.Error("Failed to parse worker configuration", "err", err)
		os.Exit(1)
	}

	bootstrap.Info("Starting Worker", "worker_id", config.FunctionsWorkerId)

	client, err := connectToHost(config.FunctionsUri, config.FunctionsGrpcMaxMessageLength, config.FunctionsWorkerId)
	if err != nil {
		bootstrap.Error("Error establishing connection to host's gRPC server", "err", err)
		os.Exit(1)
	}

	// Stand up the outbound sender goroutine and wire the LogWriter to it.
	// Errors from gRPC Send are reported via the bootstrap handler to avoid
	// a feedback loop through the writer we are setting up.
	send, stopSender, senderDone := startSender(client, bootstrap)

	logWriter := newLogWriter(send, newBootstrapHandler(os.Stderr))

	// SDK's default base handler is upgraded so user-side slog calls now
	// route through the gRPC stream as User-category logs.
	sdk.SetDefaultBaseHandler(newUserLogHandler(logWriter))

	// System logger is a separate slog.Logger that the worker's internal
	// code (dispatcher, handlers) calls via [systemLogger]. It emits
	// System-category records and never picks up invocation attrs.
	dispatcher := NewDispatcher(config, app)
	dispatcher.systemLogger = slog.New(newSystemLogHandler(logWriter))
	dispatcher.logWriter = logWriter
	dispatcher.send = send

	// Start the in-process HTTP proxy used for HTTP streaming via the
	// "HttpUri" capability. Returns nil if the app has no HTTP triggers
	// or the loopback listener can't be opened — in either case the worker
	// falls back to the gRPC-buffered HTTP path. The dispatcher detects
	// nil and skips advertising HttpUri in WorkerInitResponse.
	dispatcher.HTTPProxy = startHTTPProxy(app)

	// Recv loop. handleBidiStream owns the gRPC client.Recv call; it
	// dispatches every received message and pushes responses through the
	// outbound sender. Logs flow through logWriter independently.
	handleBidiStream(client, dispatcher, send)

	// Once Recv exits (host closed the stream or terminated us), shut the
	// sender down so any final logs drain before we return.
	stopSender()
	<-senderDone
}

// handleBidiStream reads StreamingMessages from the host and dispatches
// them. Responses are pushed through the outbound sender so they are
// serialized with any concurrent log emissions on the same gRPC stream.
//
// Each received message is processed on its own goroutine. The host
// correlates responses by function_id / invocation_id / request_id, not
// by arrival order, so worker-side ordering is irrelevant; control-plane
// sequencing (init before load, load before invocation, terminate last)
// is enforced by the host. This keeps the recv loop draining the stream
// while long-running streaming invocations are in flight, so health
// pings, env reloads, terminate, and concurrent invocations don't queue
// behind an SSE / LLM / long-poll handler. Matches the concurrency
// model used by the Python and .NET-isolated workers.
func handleBidiStream(
	client recvOnlyClient,
	disp *Dispatcher,
	send streamSender,
) {
	for {
		reqMsg, err := client.Recv()
		if err == io.EOF {
			disp.systemLogger.Info("Stream closed by server")
			return
		}
		if err != nil {
			disp.systemLogger.Error("Error receiving from stream", "err", err)
			return
		}

		go func(msg *pb.StreamingMessage) {
			respMsg, err := disp.processRequestMessage(msg)
			if err != nil {
				// Per-message errors must not crash the worker. The host
				// will retry or time out the affected request; other
				// in-flight messages keep flowing.
				disp.systemLogger.Error("Error processing request",
					"content_type", contentTypeName(msg.GetContent()), "err", err)
				return
			}
			if respMsg == nil {
				return
			}
			if sendErr := send(respMsg); sendErr != nil {
				disp.systemLogger.Error("Error sending response", "err", sendErr)
			}
		}(reqMsg)
	}
}

// recvOnlyClient is the subset of grpc.BidiStreamingClient that
// handleBidiStream uses. Defining it locally lets tests pass a fake
// without depending on the full gRPC client surface.
type recvOnlyClient interface {
	Recv() (*pb.StreamingMessage, error)
}
