package durabletask

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// Upstream keeps its protobuf types internal. Resolve their descriptors rather
// than copying the schema. These fixed histories describe individual replay
// turns, not a scheduler or a storage backend.
func newProto(t testing.TB, name protoreflect.FullName) proto.Message {
	t.Helper()
	mt, err := protoregistry.GlobalTypes.FindMessageByName(name)
	if err != nil {
		t.Fatalf("find message type %q: %v", name, err)
	}
	return mt.New().Interface()
}

func marshalHistoryEvent(t testing.TB, event map[string]any) []byte {
	t.Helper()
	if _, ok := event["timestamp"]; !ok {
		event["timestamp"] = "2026-01-01T00:00:00Z"
	}
	j, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event json: %v", err)
	}
	msg := newProto(t, "HistoryEvent")
	if err := protojson.Unmarshal(j, msg); err != nil {
		t.Fatalf("build history event: %v", err)
	}
	raw, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal history event: %v", err)
	}
	return raw
}

func orchestratorStartedEvent(t testing.TB) []byte {
	return marshalHistoryEvent(t, map[string]any{"orchestratorStarted": map[string]any{}})
}

func executionStartedEvent(t testing.TB, instanceID, name, input string) []byte {
	return marshalHistoryEvent(t, map[string]any{
		"executionStarted": map[string]any{
			"name": name, "input": input,
			"orchestrationInstance": map[string]any{"instanceId": instanceID},
		},
	})
}

var instanceCounter atomic.Uint64

func nextInstanceID() string {
	return fmt.Sprintf("test-instance-%d", instanceCounter.Add(1))
}

// firstTurnRequest encodes the two host events for a new orchestration.
func firstTurnRequest(t testing.TB, name string, input any) string {
	t.Helper()
	return encodeFirstTurn(t, nextInstanceID(), name, input)
}

func encodeFirstTurn(t testing.TB, instanceID, name string, input any) string {
	t.Helper()
	j, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}
	raw := encodeOrchestratorRequest(instanceID, nil, [][]byte{
		orchestratorStartedEvent(t),
		executionStartedEvent(t, instanceID, name, string(j)),
	})
	return base64.StdEncoding.EncodeToString(raw)
}

func encodeOrchestratorRequest(instanceID string, pastEvents, newEvents [][]byte) []byte {
	var b []byte
	b = protowire.AppendTag(b, fieldInstanceID, protowire.BytesType)
	b = protowire.AppendBytes(b, []byte(instanceID))

	for _, raw := range pastEvents {
		b = protowire.AppendTag(b, fieldPastEvents, protowire.BytesType)
		b = protowire.AppendBytes(b, raw)
	}
	for _, raw := range newEvents {
		b = protowire.AppendTag(b, fieldNewEvents, protowire.BytesType)
		b = protowire.AppendBytes(b, raw)
	}
	return b
}

func decodeRequest(t testing.TB, encoded string) (string, [][]byte, [][]byte) {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode base64 request: %v", err)
	}
	instanceID, pastEvents, newEvents, err := decodeOrchestratorRequest(raw)
	if err != nil {
		t.Fatalf("parse orchestrator request: %v", err)
	}
	return instanceID, pastEvents, newEvents
}
