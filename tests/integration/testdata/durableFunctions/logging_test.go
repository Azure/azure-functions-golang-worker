package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"sync"
	"testing"

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
}

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

// Exercise the real registered adapter, executor, SDK handler, RpcLog writer and
// otelfunc observer without requiring a Functions host or a storage backend.
func TestDurableLogging_InvocationCorrelationAndReplay(t *testing.T) {
	originalLogger := slog.Default()
	originalProvider := global.GetLoggerProvider()
	t.Cleanup(func() {
		slog.SetDefault(originalLogger)
		global.SetLoggerProvider(originalProvider)
		sdk.SetDefaultBaseHandler(nil)
	})
	slog.SetDefault(sdk.NewLogger())
	var rpcLogs []*pb.RpcLog
	writer := workerlog.NewWriter(func(message *pb.StreamingMessage) error {
		rpcLogs = append(rpcLogs, message.GetRpcLog())
		return nil
	}, nil)
	sdk.SetDefaultBaseHandler(workerlog.NewUser(writer))
	exporter := &capturedDurableLogs{}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exporter)))
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() {
		if err := lp.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	otel := otelfunc.Middleware(otelfunc.WithTracerProvider(tp), otelfunc.WithLoggerProvider(lp))
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
			invocation := fmt.Sprintf("%s-turn-%d", instance, turn)
			mc := &sdk.MiddlewareContext{InvocationContext: &sdk.InvocationContext{
				InvocationID: invocation, FunctionName: "Logged", TriggerType: "orchestrationTrigger",
			}}
			mc.SetInputString(base64.StdEncoding.EncodeToString(raw))
			before := len(exporter.records)
			if err := handler(sdk.ContextWithMiddleware(context.Background(), mc), mc); err != nil {
				t.Fatal(err)
			}
			want := 4
			offset := 0
			if turn == 1 {
				want, offset = 1, 4
			}
			logs := exporter.records[before:]
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
