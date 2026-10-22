package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azure/azure-functions-golang-worker/middleware/durabletask"
	"github.com/azure/azure-functions-golang-worker/middleware/otelfunc"
	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/task"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/protobuf/encoding/protowire"
)

type probeKey struct{}

type invocationProbe struct {
	Name       string   `json:"name"`
	Steps      []string `json:"steps"`
	ReturnSet  bool     `json:"returnSet"`
	ClientSeen bool     `json:"clientSeen"`
	Error      string   `json:"error"`
	InputSeen  string   `json:"inputSeen,omitempty"`
	Complete   bool     `json:"-"`
}

// Called with the records lock held. A snapshot must not publish an invocation
// between B:after and A:after, or before its OTel span has ended.
func completedInvocations(records map[string]*invocationProbe) map[string]*invocationProbe {
	completed := make(map[string]*invocationProbe)
	for id, record := range records {
		if record.Complete {
			completed[id] = record
		}
	}
	return completed
}

// configureOrderingProbe enables test-only functions and observation endpoints.
// The default fixture stays usable against deployed apps without these routes.
func configureOrderingProbe(app *sdk.App, durable *durabletask.Durable) bool {
	order := os.Getenv("DURABLE_TEST_MIDDLEWARE_ORDER")
	if order == "" {
		return false
	}
	if order != "before" && order != "after" {
		panic("invalid DURABLE_TEST_MIDDLEWARE_ORDER")
	}
	durable.Orchestrator("Counter", counterOrchestrator)
	durable.Orchestrator("Parent", parentOrchestrator)
	durable.Orchestrator("InputProbe", func(ctx *task.OrchestrationContext) (any, error) {
		var input int
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input, nil
	})
	durable.Orchestrator("Failing", func(*task.OrchestrationContext) (any, error) {
		return nil, errors.New("expected orchestration failure")
	})
	durable.Orchestrator("Panicking", func(*task.OrchestrationContext) (any, error) {
		panic("expected orchestrator panic")
	})
	var blockedCalls atomic.Int32
	durable.Orchestrator("Blocked", func(*task.OrchestrationContext) (any, error) {
		blockedCalls.Add(1)
		return "must not execute", nil
	})
	durable.Activity("ProbeActivity", func(ctx context.Context, input int) (int, error) {
		if ctx.Value(probeKey{}) != "A" {
			return 0, errors.New("middleware context did not reach activity")
		}
		return input + 1, nil
	})

	var mu sync.Mutex
	records := map[string]*invocationProbe{}
	observe := func(label string) sdk.Middleware {
		return sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
			return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
				if mc.FunctionName == "OrderingSnapshot" {
					return next(ctx, mc)
				}
				mu.Lock()
				p := records[mc.InvocationID]
				if p == nil {
					p = &invocationProbe{Name: mc.FunctionName}
					records[mc.InvocationID] = p
				}
				p.Steps = append(p.Steps, label+":before")
				_, present := durabletask.ClientFromContext(ctx)
				p.ClientSeen = p.ClientSeen || present
				mu.Unlock()
				if mc.FunctionName == "InputProbe" {
					// The HTTP starter supplies 0. The worker has already bound the
					// original envelope to the adapter argument at this point.
					seen, replacement := "0", "1"
					if label == "B" {
						seen, replacement = "1", "42"
					}
					encoded, err := replaceProbeInput(mc.InputString(), seen, replacement)
					if err != nil {
						return err
					}
					mc.SetInputString(encoded)
					mu.Lock()
					p.InputSeen = replacement
					mu.Unlock()
				}
				if label == "A" {
					ctx = context.WithValue(ctx, probeKey{}, "A")
				} else if ctx.Value(probeKey{}) != "A" {
					return errors.New("middleware context was lost")
				}
				err := next(ctx, mc)
				mu.Lock()
				p.Steps = append(p.Steps, label+":after")
				_, p.ReturnSet = mc.ReturnValue()
				if err != nil {
					p.Error = err.Error()
				}
				mu.Unlock()
				return err
			}
		})
	}
	exporter := tracetest.NewInMemoryExporter()
	provider := trace.NewTracerProvider(trace.WithSyncer(exporter))
	// This fixture disables host Durable tracing. Seed a known invocation parent
	// to test worker propagation independently of host tracing configuration.
	app.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, mc *sdk.MiddlewareContext) error {
			// This decorator wraps Durable, OTel and both observers in either
			// order. Completion is independent of the expected observer sequence.
			defer func() {
				mu.Lock()
				defer mu.Unlock()
				if record := records[mc.InvocationID]; record != nil {
					record.Complete = true
				}
			}()
			mc.TraceContext.TraceParent = "00-11111111111111111111111111111111-2222222222222222-01"
			return next(ctx, mc)
		}
	}))
	if order == "before" {
		app.Use(durable)
	}
	app.Use(otelfunc.Middleware(otelfunc.WithTracerProvider(provider)))
	app.Use(observe("A"))
	app.Use(observe("B"))
	app.Use(sdk.MiddlewareFunc(func(next sdk.Handler) sdk.Handler {
		return func(ctx context.Context, mc *sdk.MiddlewareContext) (err error) {
			// A user recovery decorator inside OTel converts a panic into an
			// error that the outer observers can record. The worker still owns
			// the final invocation failure response.
			defer func() {
				if p := recover(); p != nil {
					err = fmt.Errorf("recovered by middleware: %v", p)
				}
			}()
			if mc.FunctionName == "Blocked" {
				return errors.New("blocked by ordering probe")
			}
			return next(ctx, mc)
		}
	}))
	if order == "after" {
		app.Use(durable)
	}
	app.HTTP("OrderingStart", func(w http.ResponseWriter, r *http.Request) {
		client, ok := durabletask.ClientFromContext(r.Context())
		if !ok || r.Context().Value(probeKey{}) != "A" {
			http.Error(w, "missing client or context", http.StatusInternalServerError)
			return
		}
		name := instanceIDFromPath(r, "start")
		id, err := client.ScheduleNewOrchestration(r.Context(), name, api.WithInput(0))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = client.WriteCheckStatusResponse(w, r, string(id))
	}, sdk.WithRoute("ordering/start/{name}"), sdk.WithMethods("post"), sdk.WithAuth("anonymous"), durabletask.ClientInput())
	app.HTTP("OrderingSnapshot", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		type spanProbe struct {
			InvocationID string `json:"invocationId"`
			InstanceID   string `json:"instanceId"`
			TraceID      string `json:"traceId"`
			ParentID     string `json:"parentId"`
			Status       string `json:"status"`
		}
		var spans []spanProbe
		for _, s := range exporter.GetSpans() {
			p := spanProbe{TraceID: s.SpanContext.TraceID().String(), ParentID: s.Parent.SpanID().String(), Status: s.Status.Code.String()}
			for _, attr := range s.Attributes {
				switch string(attr.Key) {
				case "faas.invocation_id":
					p.InvocationID = attr.Value.AsString()
				case "durabletask.task.instance_id":
					p.InstanceID = attr.Value.AsString()
				}
			}
			spans = append(spans, p)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Invocations  map[string]*invocationProbe `json:"invocations"`
			Spans        []spanProbe                 `json:"spans"`
			BlockedCalls int32                       `json:"blockedCalls"`
		}{completedInvocations(records), spans, blockedCalls.Load()})
	}, sdk.WithRoute("ordering/snapshot"), sdk.WithMethods("get"), sdk.WithAuth("anonymous"))
	return true
}

// replaceProbeInput edits only the first-turn test orchestration's input. The
// surrounding request fields, instance ID, and event metadata are preserved.
// The protobuf types are internal to durabletask-go, so the edit is made on the
// wire format. This is a protocol probe, not a recommended application history
// transformation.
func replaceProbeInput(encoded, want, replacement string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", err
	}
	changed := 0
	// OrchestratorRequest.newEvents -> HistoryEvent.executionStarted ->
	// ExecutionStartedEvent.input -> StringValue.value
	out, err := mapBytesField(raw, 4, func(event []byte) ([]byte, error) {
		return mapBytesField(event, 3, func(started []byte) ([]byte, error) {
			return mapBytesField(started, 3, func(input []byte) ([]byte, error) {
				return mapBytesField(input, 1, func(value []byte) ([]byte, error) {
					if string(value) != want {
						return nil, fmt.Errorf("probe input = %q, want %q", value, want)
					}
					changed++
					return []byte(replacement), nil
				})
			})
		})
	})
	if err != nil {
		return "", err
	}
	if changed != 1 {
		return "", fmt.Errorf("expected one first-turn execution input, changed %d", changed)
	}
	return base64.StdEncoding.EncodeToString(out), nil
}

// mapBytesField rewrites every length-delimited field num in the message raw
// with fn, copying all other fields unchanged.
func mapBytesField(raw []byte, num protowire.Number, fn func([]byte) ([]byte, error)) ([]byte, error) {
	var out []byte
	for len(raw) > 0 {
		fieldNum, typ, n := protowire.ConsumeTag(raw)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		m := protowire.ConsumeFieldValue(fieldNum, typ, raw[n:])
		if m < 0 {
			return nil, protowire.ParseError(m)
		}
		if fieldNum == num && typ == protowire.BytesType {
			value, _ := protowire.ConsumeBytes(raw[n:])
			replaced, err := fn(value)
			if err != nil {
				return nil, err
			}
			out = protowire.AppendTag(out, fieldNum, typ)
			out = protowire.AppendBytes(out, replaced)
		} else {
			out = append(out, raw[:n+m]...)
		}
		raw = raw[n+m:]
	}
	return out, nil
}
func counterOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var count int
	if err := ctx.GetInput(&count); err != nil {
		return nil, err
	}
	if err := ctx.CallActivity("ProbeActivity", task.WithActivityInput(count)).Await(&count); err != nil {
		return nil, err
	}
	if err := ctx.CreateTimer(100 * time.Millisecond).Await(nil); err != nil {
		return nil, err
	}
	if count < 3 {
		ctx.ContinueAsNew(count)
		return nil, nil
	}
	return count, nil
}

func parentOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var count int
	if err := ctx.CallSubOrchestrator("Counter", task.WithSubOrchestratorInput(0)).Await(&count); err != nil {
		return nil, err
	}
	if count != 3 {
		return nil, fmt.Errorf("unexpected child result %d", count)
	}
	return count, nil
}
