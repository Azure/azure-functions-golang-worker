package durabletask

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/task"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Field numbers of the durabletask OrchestratorRequest message
// (orchestrator_service.proto). Only the fields needed to drive a replay are
// listed; everything else is skipped during decode.
const (
	fieldInstanceID = 1 // string instanceId
	fieldPastEvents = 3 // repeated HistoryEvent pastEvents
	fieldNewEvents  = 4 // repeated HistoryEvent newEvents
)

// orchestrationRunner replays an orchestrator from a base64-encoded
// OrchestratorRequest and returns a base64-encoded OrchestratorResponse,
// matching the contract the host's DurableTask extension expects from an
// out-of-process orchestrator function's return value.
//
// It delegates the actual replay to durabletask-go's in-process task
// executor. The only reason this file does any protobuf work itself is that
// the upstream OrchestratorRequest type is not currently exported; once it
// (or a public LoadAndRun helper) is available, loadAndRun collapses to a
// single delegation.
type orchestrationRunner struct {
	registry *task.TaskRegistry
}

// loadAndRun decodes encodedRequest (a base64 OrchestratorRequest), replays
// the orchestrator, and returns the base64-encoded OrchestratorResponse.
func (r *orchestrationRunner) loadAndRun(ctx context.Context, encodedRequest string) (string, error) {
	reqBytes, err := base64.StdEncoding.DecodeString(encodedRequest)
	if err != nil {
		return "", fmt.Errorf("durabletask: decode base64 request: %w", err)
	}

	instanceID, pastEvents, newEvents, err := decodeOrchestratorRequest(reqBytes)
	if err != nil {
		return "", fmt.Errorf("durabletask: parse orchestrator request: %w", err)
	}

	executor := task.NewTaskExecutor(r.registry)
	results, err := executor.ExecuteOrchestrator(ctx, api.InstanceID(instanceID), pastEvents, newEvents)
	if err != nil {
		return "", fmt.Errorf("durabletask: execute orchestrator: %w", err)
	}

	// Record per-turn Durable diagnostics on the active invocation span (when
	// the app uses the otelfunc middleware). Safe no-op otherwise.
	annotateTurnSpan(ctx, instanceID, pastEvents, newEvents, results)

	respBytes, err := proto.Marshal(results.Response)
	if err != nil {
		return "", fmt.Errorf("durabletask: marshal orchestrator response: %w", err)
	}
	return base64.StdEncoding.EncodeToString(respBytes), nil
}

// annotateTurnSpan records per-replay-turn Durable Task diagnostics on the
// active span — the one the otelfunc middleware started for this orchestrator
// invocation. The host dispatches every orchestrator replay as its own worker
// invocation, so each turn gets its own span and these attributes describe
// exactly one turn without overwriting a previous turn's values.
//
// It is best-effort and self-gating: trace.SpanFromContext never returns nil,
// and when the app does not register the otelfunc middleware (or any tracer)
// it returns a non-recording no-op span. IsRecording() is then false and the
// function returns before touching the span, so apps that don't opt into
// tracing pay nothing. Attribute names follow durabletask-go's durabletask.*
// convention so they sit alongside the engine's own span attributes.
func annotateTurnSpan(ctx context.Context, instanceID string, pastEvents, newEvents []*backend.HistoryEvent, results *backend.ExecutionResults) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	var actionCount int
	if results != nil && results.Response != nil {
		actionCount = len(results.Response.Actions)
	}
	span.SetAttributes(
		attribute.String("durabletask.task.instance_id", instanceID),
		attribute.Bool("durabletask.is_replay", len(pastEvents) > 0),
		attribute.Int("durabletask.history_event_count", len(pastEvents)),
		attribute.Int("durabletask.new_events_count", len(newEvents)),
		attribute.Int("durabletask.action_count", actionCount),
	)
}

// decodeOrchestratorRequest extracts the instance ID and the past / new
// history events from a marshaled OrchestratorRequest without depending on
// the upstream (currently unexported) message type. Each history-event
// sub-message is handed to backend.UnmarshalHistoryEvent, which yields the
// exported backend.HistoryEvent type the executor accepts.
//
// A repeated embedded message and the raw bytes of each element share the
// same wire encoding (length-delimited, field-tagged), so consuming fields 3
// and 4 as byte chunks and unmarshaling each chunk is wire-correct.
func decodeOrchestratorRequest(b []byte) (instanceID string, pastEvents, newEvents []*backend.HistoryEvent, err error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return "", nil, nil, protowire.ParseError(n)
		}
		b = b[n:]

		switch {
		case num == fieldInstanceID && typ == protowire.BytesType:
			v, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return "", nil, nil, protowire.ParseError(m)
			}
			instanceID = string(v)
			b = b[m:]

		case num == fieldPastEvents && typ == protowire.BytesType:
			v, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return "", nil, nil, protowire.ParseError(m)
			}
			ev, e := backend.UnmarshalHistoryEvent(v)
			if e != nil {
				return "", nil, nil, e
			}
			pastEvents = append(pastEvents, ev)
			b = b[m:]

		case num == fieldNewEvents && typ == protowire.BytesType:
			v, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return "", nil, nil, protowire.ParseError(m)
			}
			ev, e := backend.UnmarshalHistoryEvent(v)
			if e != nil {
				return "", nil, nil, e
			}
			newEvents = append(newEvents, ev)
			b = b[m:]

		default:
			m := protowire.ConsumeFieldValue(num, typ, b)
			if m < 0 {
				return "", nil, nil, protowire.ParseError(m)
			}
			b = b[m:]
		}
	}
	return instanceID, pastEvents, newEvents, nil
}

// encodeOrchestratorRequest is the inverse of decodeOrchestratorRequest. It
// is used by tests (and documents the wire format) to build an
// OrchestratorRequest payload from history events produced by a durabletask
// backend, without depending on the upstream message type.
func encodeOrchestratorRequest(instanceID string, pastEvents, newEvents []*backend.HistoryEvent) ([]byte, error) {
	var b []byte
	b = protowire.AppendTag(b, fieldInstanceID, protowire.BytesType)
	b = protowire.AppendBytes(b, []byte(instanceID))

	for _, e := range pastEvents {
		raw, err := backend.MarshalHistoryEvent(e)
		if err != nil {
			return nil, err
		}
		b = protowire.AppendTag(b, fieldPastEvents, protowire.BytesType)
		b = protowire.AppendBytes(b, raw)
	}
	for _, e := range newEvents {
		raw, err := backend.MarshalHistoryEvent(e)
		if err != nil {
			return nil, err
		}
		b = protowire.AppendTag(b, fieldNewEvents, protowire.BytesType)
		b = protowire.AppendBytes(b, raw)
	}
	return b, nil
}
