package durabletask

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/task"
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

	respBytes, err := proto.Marshal(results.Response)
	if err != nil {
		return "", fmt.Errorf("durabletask: marshal orchestrator response: %w", err)
	}
	return base64.StdEncoding.EncodeToString(respBytes), nil
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
