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

	if err := checkHistoryPresent(instanceID, pastEvents, newEvents); err != nil {
		return "", err
	}

	executor := task.NewTaskExecutor(r.registry)
	results, err := executor.ExecuteOrchestrator(ctx, api.InstanceID(instanceID), pastEvents, newEvents)
	if err != nil {
		return "", fmt.Errorf("durabletask: execute orchestrator: %w", err)
	}

	assignSubOrchestrationIDs(instanceID, results)

	// Record per-turn Durable diagnostics on the active invocation span (when
	// the app uses the otelfunc middleware). Safe no-op otherwise.
	annotateTurnSpan(ctx, instanceID, pastEvents, newEvents, results)

	respBytes, err := proto.Marshal(results.Response)
	if err != nil {
		return "", fmt.Errorf("durabletask: marshal orchestrator response: %w", err)
	}
	return base64.StdEncoding.EncodeToString(respBytes), nil
}

// assignSubOrchestrationIDs fills in the instance ID of any sub-orchestration
// the orchestrator scheduled without naming one explicitly.
//
// Responsibility for this is split differently in each Durable Task stack. In
// standalone durabletask-go the *backend* fills the gap: runtimestate.go
// assigns "<parent>:<action id in hex>" when CreateSubOrchestration carries an
// empty instance ID. The .NET SDK instead does it in the *worker*, where
// TaskOrchestrationContextWrapper.CallSubOrchestratorAsync falls back to a
// replay-safe generated ID.
//
// Here the backend is the Functions host's DurableTask extension, which does
// neither: it forwards CreateSubOrchestrationAction with InstanceId = "" and
// the resulting message is never delivered, so the child never starts and the
// parent waits forever with no error anywhere. Filling the ID in the worker,
// as .NET does, closes that gap.
//
// The format deliberately matches durabletask-go's backend so an orchestrator
// produces the same child instance IDs under the Functions host as it does
// standalone. It is derived from the parent instance ID and the action's
// sequence number, both of which are stable across replays.
func assignSubOrchestrationIDs(instanceID string, results *backend.ExecutionResults) {
	if results == nil || results.Response == nil {
		return
	}
	for _, action := range results.Response.GetActions() {
		createSO := action.GetCreateSubOrchestration()
		if createSO != nil && createSO.GetInstanceId() == "" {
			createSO.InstanceId = fmt.Sprintf("%s:%04x", instanceID, action.GetId())
		}
	}
}

// checkHistoryPresent rejects a request whose orchestration history the host
// withheld.
//
// Every request carries an ExecutionStarted event somewhere: the host's
// OutOfProcMiddleware refuses to dispatch an orchestration whose runtime state
// is missing one. On the first turn of an execution it arrives in newEvents
// and pastEvents is empty; on every later turn pastEvents is non-empty. A
// ContinueAsNew generation starts a fresh execution and so looks like a first
// turn, which satisfies the same rule.
//
// That leaves exactly one way to observe empty pastEvents with no
// ExecutionStarted in newEvents: the host deliberately omitted the history and
// expects the worker to have cached the session. That is the extended-sessions
// protocol, in which the host sets IncludeState=false in the request's
// properties map and expects OrchestratorResponse.requiresHistory in reply.
//
// This worker implements neither side of that exchange, and today it cannot be
// asked to: the extension's DurableTaskOptions.Validate refuses to start the
// host when extendedSessionsEnabled is true and FUNCTIONS_WORKER_RUNTIME is
// anything other than dotnet or dotnet-isolated, and the value is baked into
// the Functions Go image as "native". Replaying an empty history would
// re-schedule work that already completed, so if that guarantee ever changes
// upstream this fails loudly instead of silently duplicating activities.
func checkHistoryPresent(instanceID string, pastEvents, newEvents []*backend.HistoryEvent) error {
	if len(pastEvents) > 0 {
		return nil
	}
	for _, event := range newEvents {
		if event.GetExecutionStarted() != nil {
			return nil
		}
	}
	return fmt.Errorf("durabletask: orchestration %q was dispatched without history and without an "+
		"ExecutionStarted event; the host withheld the history, which happens only with extended "+
		"sessions. This worker does not cache orchestration sessions, and replaying an empty history "+
		"would re-run completed work. Set extendedSessionsEnabled to false in host.json", instanceID)
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
	// durabletask.task.instance_id matches the attribute both durabletask-go
	// and the .NET SDK emit. The counts below are specific to this worker:
	// neither SDK traces individual replay turns, so there is no established
	// convention to follow for them.
	//
	// There is deliberately no is_replay attribute. The SDKs' IsReplaying is
	// point-in-time — it flips partway through a turn as the executor moves
	// from reconciling history to running new work — so no single value is
	// correct for a span that covers the whole turn. history_event_count
	// already carries the only fact available here, and is zero on the first
	// turn of an execution.
	span.SetAttributes(
		attribute.String("durabletask.task.instance_id", instanceID),
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
