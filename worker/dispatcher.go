package worker

import (
	"context"
	"log/slog"
	"sync"

	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// Dispatcher routes incoming gRPC StreamingMessages to the appropriate
// handlers. It also owns the LogWriter and the worker-internal
// systemLogger, which are populated by [Start] before the recv loop
// begins.
type Dispatcher struct {
	WorkerStartupConfig *WorkerStartupConfig
	App                 *sdk.App
	LoadedFunctions     *sync.Map
	// HTTPProxy is the in-process HTTP server used for HTTP streaming. It is
	// nil when the user app has no HTTP triggers, or when the loopback
	// listener could not be opened (worker falls back to gRPC body).
	HTTPProxy *httpProxy

	// systemLogger is the worker-internal slog.Logger used for
	// dispatcher/handler diagnostics. It emits System-category RpcLog
	// records over the gRPC stream. Tests construct a Dispatcher via
	// [NewDispatcher] and either set this field directly or rely on
	// [Dispatcher.SystemLogger] which lazily falls back to a discard
	// handler.
	systemLogger *slog.Logger

	// logWriter is the writer the system and user log handlers push
	// records through. Populated by Start once the gRPC stream is open.
	logWriter *LogWriter

	// send is the goroutine-safe enqueue function for the gRPC outbound
	// channel. Populated by Start. Made available on the dispatcher so
	// concurrent paths (e.g. the future HTTP-streaming bridge) can push
	// responses without bypassing the sender goroutine.
	send streamSender
}

// NewDispatcher constructs a Dispatcher. The caller is expected to set
// systemLogger / logWriter / send after construction (typically via
// [Start]) before driving processRequestMessage.
func NewDispatcher(workerStartupConfig *WorkerStartupConfig, app *sdk.App) *Dispatcher {
	return &Dispatcher{
		WorkerStartupConfig: workerStartupConfig,
		App:                 app,
		LoadedFunctions:     &sync.Map{},
	}
}

// SystemLogger returns the worker-internal logger, falling back to a
// discard logger if Start has not yet wired one in. Tests that exercise
// processRequestMessage without a full Start can rely on the fallback.
func (disp *Dispatcher) SystemLogger() *slog.Logger {
	if disp.systemLogger != nil {
		return disp.systemLogger
	}
	return slog.New(discardHandler{})
}

func (disp *Dispatcher) processRequestMessage(reqMsg *pb.StreamingMessage) (*pb.StreamingMessage, error) {
	requestId := disp.WorkerStartupConfig.FunctionsRequestId
	logger := disp.SystemLogger()

	switch content := reqMsg.GetContent().(type) {
	case *pb.StreamingMessage_WorkerInitRequest:
		logger.LogAttrs(context.Background(), slog.LevelInfo, "Handling WorkerInitRequest")
		// Wire host-supplied log_categories into the LogWriter's filter
		// map so subsequent records are filtered as the host expects.
		if disp.logWriter != nil {
			disp.logWriter.SetCategories(content.WorkerInitRequest.GetLogCategories())
		}
		return handleWorkerInitRequest(content.WorkerInitRequest, requestId, disp), nil
	case *pb.StreamingMessage_FunctionsMetadataRequest:
		logger.LogAttrs(context.Background(), slog.LevelInfo, "Handling FunctionsMetadataRequest")
		return handleFunctionsMetadataRequest(content.FunctionsMetadataRequest, disp.App, requestId), nil
	case *pb.StreamingMessage_InvocationRequest:
		logger.LogAttrs(context.Background(), slog.LevelInfo, "Handling InvocationRequest",
			slog.String("invocation_id", content.InvocationRequest.GetInvocationId()),
		)
		// For invocations, the Host *does* provide a specific RequestId that we MUST echo back exactly.
		return handleInvocationRequest(content.InvocationRequest, disp, reqMsg.GetRequestId())
	case *pb.StreamingMessage_FunctionLoadRequest:
		logger.LogAttrs(context.Background(), slog.LevelInfo, "Handling FunctionLoadRequest",
			slog.String("function_id", content.FunctionLoadRequest.GetFunctionId()),
		)
		return handleFunctionLoadRequest(content.FunctionLoadRequest, disp, requestId), nil
	case *pb.StreamingMessage_WorkerStatusRequest:
		logger.LogAttrs(context.Background(), slog.LevelDebug, "Handling WorkerStatusRequest")
		return handleWorkerStatusRequest(requestId, content.WorkerStatusRequest)
	case *pb.StreamingMessage_WorkerTerminate:
		logger.LogAttrs(context.Background(), slog.LevelInfo, "Handling WorkerTerminate")
		return handleWorkerTerminate(requestId, content.WorkerTerminate)
	case *pb.StreamingMessage_FunctionEnvironmentReloadRequest:
		logger.LogAttrs(context.Background(), slog.LevelInfo, "Handling FunctionEnvironmentReloadRequest")
		return handleFunctionEnvironmentReloadRequest(requestId, content.FunctionEnvironmentReloadRequest)
	default:
		logger.LogAttrs(context.Background(), slog.LevelWarn, "Received unhandled message type",
			slog.String("content_type", contentTypeName(content)),
		)
		return nil, nil
	}
}

// contentTypeName provides a human-readable type name for unknown
// StreamingMessage payloads, used in log diagnostics.
func contentTypeName(content any) string {
	if content == nil {
		return "<nil>"
	}
	return slog.AnyValue(content).String()
}

// discardHandler implements slog.Handler by ignoring every record. Used
// as the safe fallback when a Dispatcher is constructed without a full
// Start (test scenarios).
type discardHandler struct{}

func (discardHandler) Enabled(_ context.Context, _ slog.Level) bool  { return false }
func (discardHandler) Handle(_ context.Context, _ slog.Record) error { return nil }
func (discardHandler) WithAttrs(_ []slog.Attr) slog.Handler          { return discardHandler{} }
func (discardHandler) WithGroup(_ string) slog.Handler               { return discardHandler{} }
