package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/azure/azure-functions-golang-worker/middleware/durabletask"
	"github.com/azure/azure-functions-golang-worker/middleware/otelfunc"
	"github.com/azure/azure-functions-golang-worker/sdk"
	workerlog "github.com/azure/azure-functions-golang-worker/worker/log"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
	"github.com/microsoft/durabletask-go/task"
	olog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
)

type capturedDurableLogs struct {
	mu      sync.Mutex
	records []sdklog.Record
	rpcLogs []*pb.RpcLog
}

// The worker observer is process-wide and retains its first OTel logger.
// Reuse a synchronous provider across tests rather than shutting down a
// provider the observer will still use on the next test or -count iteration.
var durableLogCapture capturedDurableLogs
var durableLogProvider = sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(&durableLogCapture)))

func (c *capturedDurableLogs) Export(_ context.Context, records []sdklog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, record := range records {
		c.records = append(c.records, record.Clone())
	}
	return nil
}

func (*capturedDurableLogs) Shutdown(context.Context) error   { return nil }
func (*capturedDurableLogs) ForceFlush(context.Context) error { return nil }

func (c *capturedDurableLogs) snapshot() ([]sdklog.Record, []*pb.RpcLog) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]sdklog.Record(nil), c.records...), append([]*pb.RpcLog(nil), c.rpcLogs...)
}

func setupDurableLogging(t *testing.T) (*capturedDurableLogs, sdk.Middleware) {
	t.Helper()
	originalLogger := slog.Default()
	originalProvider := global.GetLoggerProvider()
	t.Cleanup(func() {
		slog.SetDefault(originalLogger)
		global.SetLoggerProvider(originalProvider)
		sdk.SetDefaultBaseHandler(nil)
	})
	slog.SetDefault(sdk.NewLogger())
	exporter := &durableLogCapture
	exporter.mu.Lock()
	exporter.records, exporter.rpcLogs = nil, nil
	exporter.mu.Unlock()
	writer := workerlog.NewWriter(func(message *pb.StreamingMessage) error {
		exporter.mu.Lock()
		defer exporter.mu.Unlock()
		exporter.rpcLogs = append(exporter.rpcLogs, message.GetRpcLog())
		return nil
	}, nil)
	sdk.SetDefaultBaseHandler(workerlog.NewUser(writer))
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return exporter, otelfunc.Middleware(otelfunc.WithTracerProvider(tp), otelfunc.WithLoggerProvider(durableLogProvider))
}

func loggingRequest(t *testing.T, instance string, turn int) string {
	t.Helper()
	start := fmt.Sprintf(`{"executionStarted":{"name":"Logged","orchestrationInstance":{"instanceId":%q}}}`, instance)
	past, next := `[]`, `[{"orchestratorStarted":{}},`+start+`]`
	if turn == 1 {
		past = `[{"orchestratorStarted":{}},` + start + `,{"eventId":0,"taskScheduled":{"name":"Echo"}}]`
		next = `[{"orchestratorStarted":{}},{"taskCompleted":{"taskScheduledId":0,"result":"\"done\""}}]`
	}
	messageType, err := protoregistry.GlobalTypes.FindMessageByName("OrchestratorRequest")
	if err != nil {
		t.Fatal(err)
	}
	request := messageType.New().Interface()
	if err := protojson.Unmarshal([]byte(fmt.Sprintf(`{"instanceId":%q,"pastEvents":%s,"newEvents":%s}`, instance, past, next)), request); err != nil {
		t.Fatal(err)
	}
	raw, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// Exercise the real registered adapter, executor, SDK handler, RpcLog writer and
// otelfunc observer without requiring a Functions host or a storage backend.
func TestDurableLogging_InvocationCorrelationAndReplay(t *testing.T) {
	exporter, otel := setupDurableLogging(t)
	durable := durabletask.Middleware(durabletask.WithOrchestrator("Logged", func(ctx *task.OrchestrationContext) (any, error) {
		if _, ok := sdk.FromContext(ctx.Context()); ok || trace.SpanContextFromContext(ctx.Context()).IsValid() {
			return nil, fmt.Errorf("host invocation context leaked into orchestrator")
		}
		logger := ctx.Logger().With("tenant", "example").WithGroup("step").With("phase", "before")
		logger.Debug("debug")
		logger.Info("info")
		logger.Warn("warn")
		logger.Error("error")
		var result string
		if err := ctx.CallActivity("Echo").Await(&result); err != nil {
			return nil, err
		}
		ctx.Logger().InfoContext(ctx.Context(), "after")
		return result, nil
	}))
	adapter := durable.ProvidedFunctions()[0].Func.(func(context.Context, string) (string, error))
	var invocationSpan trace.SpanContext
	handler := otel.Wrap(func(ctx context.Context, mc *sdk.MiddlewareContext) error {
		invocationSpan = trace.SpanContextFromContext(ctx)
		_, err := adapter(ctx, mc.InputString())
		return err
	})
	expectedLevels := []olog.Severity{olog.SeverityDebug, olog.SeverityInfo, olog.SeverityWarn, olog.SeverityError, olog.SeverityInfo}
	expectedBodies := []string{"debug", "info", "warn", "error", "after"}
	expectedRPC := []pb.RpcLog_Level{pb.RpcLog_Debug, pb.RpcLog_Information, pb.RpcLog_Warning, pb.RpcLog_Error, pb.RpcLog_Information}
	for _, instance := range []string{"first", "second"} {
		for turn := 0; turn < 2; turn++ {
			invocation := fmt.Sprintf("%s-turn-%d", instance, turn)
			mc := &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{
				InvocationID: invocation, FunctionName: "Logged", TriggerType: "orchestrationTrigger",
			}}
			mc.SetInputString(loggingRequest(t, instance, turn))
			previous, _ := exporter.snapshot()
			before := len(previous)
			if err := handler(sdk.ContextWithMiddleware(context.Background(), mc), mc); err != nil {
				t.Fatal(err)
			}
			want := 4
			offset := 0
			if turn == 1 {
				want, offset = 1, 4
			}
			records, rpcLogs := exporter.snapshot()
			logs := records[before:]
			if len(logs) != want || len(rpcLogs) != before+want {
				t.Fatalf("%s: got %d OTel logs and %d total RpcLogs, want %d new logs", invocation, len(logs), len(rpcLogs), want)
			}
			for i, record := range logs {
				if record.Body().AsString() != expectedBodies[offset+i] || record.Severity() != expectedLevels[offset+i] {
					t.Errorf("unexpected OTel body or severity: %v %v", record.Body(), record.Severity())
				}
				if !invocationSpan.IsValid() || record.TraceID() != invocationSpan.TraceID() || record.SpanID() != invocationSpan.SpanID() {
					t.Error("Durable log does not carry the current invocation's exact trace/span IDs")
				}
				attrs := map[string]string{}
				record.WalkAttributes(func(kv olog.KeyValue) bool {
					attrs[kv.Key] = kv.Value.AsString()
					return true
				})
				if attrs["invocation_id"] != invocation || attrs["function_name"] != "Logged" ||
					attrs["trigger_type"] != "orchestrationTrigger" || attrs["durabletask.instance_id"] != instance {
					t.Errorf("missing or stale invocation/Durable attributes: %v", attrs)
				}
				if turn == 0 && (attrs["tenant"] != "example" || attrs["step.phase"] != "before") {
					t.Errorf("bound attributes/groups lost: %v", attrs)
				}
				rpc := rpcLogs[before+i]
				if rpc.InvocationId != invocation || rpc.LogCategory != pb.RpcLog_User || rpc.Category != "Function.Logged" ||
					rpc.Level != expectedRPC[offset+i] {
					t.Errorf("unexpected RpcLog: %v", rpc)
				}
			}

		}
	}
}

func TestDurableLogging_ConcurrentInvocationIsolation(t *testing.T) {
	exporter, otel := setupDurableLogging(t)
	const count = 4
	entered := make(chan struct{}, count)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	deadline, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var workers sync.WaitGroup
	defer func() {
		cancel()
		unblock()
		workers.Wait()
	}()
	durable := durabletask.Middleware(durabletask.WithOrchestrator("Logged", func(ctx *task.OrchestrationContext) (any, error) {
		logger := ctx.Logger()
		logger.Info("before overlap")
		// Hold every invocation inside its replay so all loggers coexist.
		entered <- struct{}{}
		select {
		case <-release:
		case <-deadline.Done():
			return nil, deadline.Err()
		}
		logger.InfoContext(ctx.Context(), "after overlap")
		return nil, nil
	}))
	adapter := durable.ProvidedFunctions()[0].Func.(func(context.Context, string) (string, error))
	type result struct {
		id   string
		span trace.SpanContext
		err  error
	}
	results := make(chan result, count)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("concurrent-%d", i)
		request := loggingRequest(t, id, 0)
		workers.Add(1)
		go func() {
			defer workers.Done()
			var span trace.SpanContext
			handler := otel.Wrap(func(ctx context.Context, mc *sdk.MiddlewareContext) error {
				span = trace.SpanContextFromContext(ctx)
				_, err := adapter(ctx, mc.InputString())
				return err
			})
			mc := &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{
				InvocationID: id, FunctionName: "Logged", TriggerType: "orchestrationTrigger",
			}}
			mc.SetInputString(request)
			err := handler(sdk.ContextWithMiddleware(deadline, mc), mc)
			results <- result{id: id, span: span, err: err}
		}()
	}
	// Release only after every replay has logged and is still in flight.
	for i := 0; i < count; i++ {
		select {
		case <-entered:
		case <-deadline.Done():
			t.Fatal("concurrent replays did not reach the overlap barrier")
		}
	}
	unblock()
	spans := make(map[string]trace.SpanContext, count)
	spanOwners := make(map[trace.SpanID]string, count)
	for i := 0; i < count; i++ {
		select {
		case got := <-results:
			if got.err != nil || !got.span.IsValid() {
				t.Fatalf("%s: invocation error %v, span %v", got.id, got.err, got.span)
			}
			if owner, ok := spanOwners[got.span.SpanID()]; ok {
				t.Errorf("%s and %s share an invocation span", owner, got.id)
			}
			spanOwners[got.span.SpanID()] = got.id
			spans[got.id] = got.span
		case <-deadline.Done():
			t.Fatal("concurrent replays did not complete")
		}
	}
	records, rpcLogs := exporter.snapshot()
	if len(records) != 2*count || len(rpcLogs) != 2*count {
		t.Fatalf("got %d OTel records and %d RpcLogs, want %d each", len(records), len(rpcLogs), 2*count)
	}
	otelCounts, rpcCounts := map[string]int{}, map[string]int{}
	for _, record := range records {
		attrs := map[string]string{}
		record.WalkAttributes(func(kv olog.KeyValue) bool {
			attrs[kv.Key] = kv.Value.AsString()
			return true
		})
		id := attrs["durabletask.instance_id"]
		span, ok := spans[id]
		if !ok || attrs["invocation_id"] != id || attrs["function_name"] != "Logged" ||
			record.TraceID() != span.TraceID() || record.SpanID() != span.SpanID() {
			t.Errorf("cross-invocation OTel correlation: attrs=%v trace=%s span=%s", attrs, record.TraceID(), record.SpanID())
		}
		otelCounts[id+"/"+record.Body().AsString()]++
	}
	for _, record := range rpcLogs {
		if _, ok := spans[record.InvocationId]; !ok || record.Category != "Function.Logged" ||
			record.PropertiesMap["durabletask.instance_id"].GetString_() != record.InvocationId {
			t.Errorf("unexpected RpcLog correlation: %v", record)
		}
		rpcCounts[record.InvocationId]++
	}
	for id := range spans {
		if otelCounts[id+"/before overlap"] != 1 || otelCounts[id+"/after overlap"] != 1 || rpcCounts[id] != 2 {
			t.Errorf("%s: expected one log before and after overlap and two RpcLogs, got OTel=%v RpcLog=%d", id, otelCounts, rpcCounts[id])
		}
	}
}
