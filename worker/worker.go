package worker

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker/log"
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
//  2. As soon as the gRPC stream is open, Start constructs a [log.Writer]
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
	bootstrapHandler := log.NewBootstrap(os.Stderr)
	bootstrap := slog.New(bootstrapHandler)

	config, err := GetWorkerStartupConfig()
	if err != nil {
		bootstrap.LogAttrs(context.Background(), slog.LevelError, "Failed to parse worker configuration",
			slog.Any("err", err),
		)
		os.Exit(1)
	}

	bootstrap.LogAttrs(context.Background(), slog.LevelInfo, "Starting Worker",
		slog.String("worker_id", config.FunctionsWorkerId),
	)

	client, err := connectToHost(config.FunctionsUri, config.FunctionsGrpcMaxMessageLength, config.FunctionsWorkerId)
	if err != nil {
		bootstrap.LogAttrs(context.Background(), slog.LevelError, "Error establishing connection to host's gRPC server",
			slog.Any("err", err),
		)
		os.Exit(1)
	}

	// Stand up the outbound sender goroutine and wire the log.Writer to it.
	// Errors from gRPC Send are reported via the bootstrap handler to avoid
	// a feedback loop through the writer we are setting up.
	send, stopSender, senderDone := startSender(client, bootstrap)

	// Reuse the same bootstrap handler instance for both pre-gRPC logs and
	// the post-gRPC fallback path. Using a single handler ensures all
	// stderr writes are serialized by a single mutex, preventing byte-level
	// interleaving of concurrent log lines.
	logWriter := log.NewWriter(send, bootstrapHandler)

	// SDK's default base handler is upgraded so user-side slog calls now
	// route through the gRPC stream as User-category logs.
	sdk.SetDefaultBaseHandler(log.NewUser(logWriter))

	// System logger is a separate slog.Logger that the worker's internal
	// code (dispatcher, handlers) calls via [systemLogger]. It emits
	// System-category records and never picks up invocation attrs.
	dispatcher := NewDispatcher(config, app)
	dispatcher.systemLogger = slog.New(log.NewSystem(logWriter))
	dispatcher.logWriter = logWriter
	dispatcher.send = send

	// Start the in-process HTTP proxy used for HTTP streaming via the
	// "HttpUri" capability. Returns nil if the app has no HTTP triggers
	// or the loopback listener can't be opened — in either case the worker
	// falls back to the gRPC-buffered HTTP path. The dispatcher detects
	// nil and skips advertising HttpUri in WorkerInitResponse.
	dispatcher.HTTPProxy = startHTTPProxy(app)
	if dispatcher.HTTPProxy != nil {
		// Wire the system logger into the HTTP proxy so all logging is
		// consistent and participates in the RpcLog pipeline.
		dispatcher.HTTPProxy.systemLogger = dispatcher.systemLogger
	}

	// Emit a one-time record summarizing the worker build so customers
	// can correlate observed behavior with the SDK version and the git
	// commit their binary was built from. We route through slog.Default
	// (i.e. the user-category gRPC handler installed above), not the
	// System logger, because:
	//
	//   1. With telemetryMode = OpenTelemetry, the host does NOT forward
	//      System-category RpcLogs into its OTel log pipeline (per
	//      WorkerOpenTelemetryEnabled — see middleware/otelfunc godoc).
	//      A System record would be invisible to OTel customers, who
	//      are the population most likely to query for it.
	//   2. The user-category path runs through [log.NewUser]'s handler, which
	//      writes the RpcLog AND fans the slog.Record out to any
	//      registered [log.Observer]. middleware/otelfunc registers
	//      an observer that bridges to the global OTel LoggerProvider,
	//      so the record reaches the configured backend (e.g. New Relic)
	//      automatically when the customer is using OTel.
	//   3. Classic Application Insights customers still see the record:
	//      it lands under the app's Function.* category instead of the
	//      "Worker" category, both of which the host enables at
	//      Information by default. Same customer query (`message
	//      startswith "Go worker started"`) finds it in both modes.
	md := buildWorkerMetadata()
	slog.LogAttrs(context.Background(), slog.LevelInfo, "Go worker started",
		slog.String("sdk_version", md.GetWorkerVersion()),
		slog.String("sdk_replaced", md.GetCustomProperties()[MetaSDKReplaced]),
		slog.String("sdk_replace_path", md.GetCustomProperties()[MetaSDKReplacePath]),
		slog.String("vcs_revision", md.GetCustomProperties()[MetaAppVCSRevision]),
		slog.String("build_dirty", md.GetCustomProperties()[MetaAppBuiltDirty]),
		slog.String("go_version", md.GetRuntimeVersion()),
		slog.String("worker_bitness", md.GetWorkerBitness()),
		slog.Bool("http_proxy_enabled", dispatcher.HTTPProxy != nil),
	)

	// Trap SIGTERM / SIGINT so middleware-owned resources (e.g. OTel
	// providers) get a chance to flush and shut down cleanly when the
	// host or platform asks the worker to terminate. On signal the
	// supporting goroutine below calls client.CloseSend(), which causes
	// the in-flight client.Recv() to return io.EOF; handleBidiStream
	// exits via its normal stream-closed path and Start falls through
	// to the shutdown sequence below.
	signalCtx, signalStop := signalContext(dispatcher.systemLogger)
	defer signalStop()

	// Recv loop. handleBidiStream owns the gRPC client.Recv call; it
	// dispatches every received message and pushes responses through the
	// outbound sender. Logs flow through logWriter independently.
	go func() {
		<-signalCtx.Done()
		// Cancel reads by closing the gRPC client so handleBidiStream
		// returns. We rely on the standard io.EOF path below to log
		// "Stream closed" and proceed to shutdown.
		_ = client.CloseSend()
	}()
	handleBidiStream(client, dispatcher, send)

	// Once Recv exits (host closed the stream or terminated us), shut the
	// sender down so any final logs drain before we return.
	stopSender()
	<-senderDone

	// Run middleware-registered shutdowns. Bounded so a misbehaving
	// exporter cannot delay process exit indefinitely.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := app.RunShutdowns(shutdownCtx); err != nil {
		dispatcher.systemLogger.LogAttrs(shutdownCtx, slog.LevelWarn, "Middleware shutdown returned error",
			slog.Any("err", err),
		)
	}
}

// signalContext returns a context that is cancelled when the process
// receives SIGTERM or SIGINT (Ctrl-C in interactive sessions, host-driven
// termination in production). The returned stop function should be deferred
// to release the signal handler when the worker exits via the normal stream
// close path.
func signalContext(logger *slog.Logger) (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case sig := <-ch:
			logger.LogAttrs(ctx, slog.LevelInfo, "Received termination signal; initiating shutdown",
				slog.String("signal", sig.String()),
			)
			cancel()
		case <-ctx.Done():
		}
	}()
	stop := func() {
		signal.Stop(ch)
		cancel()
	}
	return ctx, stop
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
			disp.systemLogger.LogAttrs(context.Background(), slog.LevelInfo, "Stream closed by server")
			return
		}
		if err != nil {
			disp.systemLogger.LogAttrs(context.Background(), slog.LevelError, "Error receiving from stream",
				slog.Any("err", err),
			)
			return
		}

		go func(msg *pb.StreamingMessage) {
			// Recover from any panic the user handler raises so one
			// misbehaving function doesn't crash the worker. The host
			// will see no response for the affected invocation (or
			// time it out). Other in-flight goroutines are unaffected
			// because Go panics are goroutine-local.
			//
			// We deliberately don't try to construct a Failure
			// InvocationResponse from the recovered value: the panicking
			// path is far from the normal response-building code and
			// reconstructing the InvocationId, RequestId, etc. here
			// duplicates logic in handleInvocationRequest. The host
			// already handles the missing-response case; surfacing the
			// panic via the system logger is the most useful action.
			defer func() {
				if rec := recover(); rec != nil {
					disp.systemLogger.LogAttrs(context.Background(), slog.LevelError, "panic in message dispatch goroutine",
						slog.String("content_type", contentTypeName(msg.GetContent())),
						slog.Any("panic", rec),
						slog.String("stack", string(debug.Stack())),
					)
				}
			}()

			respMsg, err := disp.processRequestMessage(msg)
			if err != nil {
				// Per-message errors must not crash the worker. The host
				// will retry or time out the affected request; other
				// in-flight messages keep flowing.
				disp.systemLogger.LogAttrs(context.Background(), slog.LevelError, "Error processing request",
					slog.String("content_type", contentTypeName(msg.GetContent())),
					slog.Any("err", err),
				)
				return
			}
			if respMsg == nil {
				return
			}
			if sendErr := send(respMsg); sendErr != nil {
				disp.systemLogger.LogAttrs(context.Background(), slog.LevelError, "Error sending response",
					slog.Any("err", sendErr),
				)
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
