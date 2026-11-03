package durabletask

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/payload"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type urlPayloadStore struct {
	*payload.MemoryStore
	value []byte
}

const customPayloadURL = "https://payload.example.test/opaque-token"

func (s *urlPayloadStore) StoreToken(_ context.Context, value []byte) (string, error) {
	s.value = append([]byte(nil), value...)
	return customPayloadURL, nil
}

func (s *urlPayloadStore) ResolveToken(_ context.Context, _ string) ([]byte, error) {
	return s.value, nil
}

func (*urlPayloadStore) IsLargePayloadToken(value string) bool { return value == customPayloadURL }

func TestFunctionsCustomPayloadTokens(t *testing.T) {
	store := &urlPayloadStore{MemoryStore: payload.NewMemoryStore()}
	options := mustNormalizeLargePayloads(&api.LargePayloadOptions{
		Store: store, Resolver: store, TokenStore: store, ThresholdBytes: 8, MaxPayloadBytes: 1024,
	})
	for _, value := range []string{`"long application string"`, `{"value":"long application object"}`} {
		connection := &payloadConnection{}
		client := NewClient(connection, WithClientLargePayloads(options))
		if _, err := client.ScheduleNewOrchestration(t.Context(), "echo", api.WithRawInput(value)); err != nil {
			t.Fatal(err)
		}
		var wire map[string]functionsPayloadReference
		if err := json.Unmarshal([]byte(connection.input), &wire); err != nil ||
			wire[payloadEnvelopeKey].Reference != customPayloadURL || wire[payloadEnvelopeKey].Version != 1 {
			t.Fatalf("custom management token was not enveloped: %q, %v", connection.input, err)
		}
		got, err := transformPayload(t.Context(), options, connection.input, false)
		if err != nil || got != value {
			t.Fatalf("execution hydration = %q, %v; want %q", got, err, value)
		}
		if _, err := transformPayload(t.Context(), memoryPayloadOptions(t), connection.input, false); !errors.Is(err, api.ErrLargePayloadReference) {
			t.Fatalf("unrecognized custom envelope = %v", err)
		}
		result := newProto(t, "ExecutionCompletedEvent")
		setStringWrapper(result.ProtoReflect(), "result", value)
		if err := transformPayloadMessage(t.Context(), options, result.ProtoReflect(), true); err != nil {
			t.Fatal(err)
		}
		resultWire := stringWrapperField(result.ProtoReflect(), "result")
		if resultWire != connection.input {
			t.Fatalf("execution envelope %q differs from management %q", resultWire, connection.input)
		}
		underlying := &payloadTestStream{received: result}
		stream := functionsPayloadStream{
			ClientStream: underlying,
			tokenStore:   store,
		}
		request := newProto(t, "CreateInstanceRequest")
		setStringWrapper(request.ProtoReflect(), "input", customPayloadURL)
		if err := stream.SendMsg(request); err != nil {
			t.Fatal(err)
		}
		if got := stringWrapperField(underlying.sent.ProtoReflect(), "input"); got != resultWire {
			t.Fatalf("streamed outgoing custom token = %q; want %q", got, resultWire)
		}
		received := newProto(t, "ExecutionCompletedEvent")
		if err := stream.RecvMsg(received); err != nil {
			t.Fatal(err)
		}
		if got := stringWrapperField(received.ProtoReflect(), "result"); got != customPayloadURL {
			t.Fatalf("streamed custom token = %q", got)
		}
		metadata, err := client.FetchOrchestrationMetadata(t.Context(), "id", api.WithFetchPayloads(true))
		if err != nil || metadata.SerializedOutput != value {
			t.Fatalf("management hydration = %#v, %v", metadata, err)
		}
	}
	literal := `"` + customPayloadURL + `"`
	if got, err := wrapPayloadReference(literal, store); err != nil || got != literal {
		t.Fatalf("quoted application URL changed: %q, %v", got, err)
	}
}

func TestFunctionsPayloadEnvelopeAndCollisions(t *testing.T) {
	options := memoryPayloadOptions(t)
	value := `"application string"`
	token, err := transformPayload(t.Context(), options, value, true)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := wrapPayloadReference(token, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid([]byte(envelope)) || envelope[0] != '{' {
		t.Fatalf("reference is not a JSON object: %q", envelope)
	}
	hydrated, err := transformPayload(t.Context(), options, envelope, false)
	if err != nil || hydrated != value {
		t.Fatalf("envelope hydration=%q error=%v", hydrated, err)
	}
	if _, err := transformPayload(t.Context(), nil, envelope, false); !errors.Is(err, api.ErrFeatureNotSupported) {
		t.Fatalf("disabled envelope error=%v", err)
	}
	for _, malformed := range []string{
		`{"$durabletaskPayload":null}`,
		`{"$durabletaskPayload":"blob:v2:value"}`,
		`{"$durabletaskPayload":{"version":2,"reference":"blob:v2:value"}}`,
		`{"$durabletaskPayload":{"version":1,"reference":"ordinary value"}}`,
		`{"$durabletaskPayload":{"version":1,"reference":"blob:v2:value","unknown":1}}`,
		`{"$durabletaskPayload":{"version":1,"reference":"blob:v2:value"},"extra":true}`,
	} {
		if _, err := transformPayload(t.Context(), options, malformed, false); !errors.Is(err, api.ErrLargePayloadReference) {
			t.Fatalf("malformed envelope %s: %v", malformed, err)
		}
	}
	// All top-level uses of the reserved key are rejected as application
	// output, even a structurally valid envelope. This is not heuristic hydration.
	for _, collision := range []string{envelope, `{"$durabletaskPayload":"application object"}`} {
		for _, threshold := range []int{8, 1024} {
			copy := *options
			copy.ThresholdBytes = threshold
			if _, err := transformPayload(t.Context(), &copy, collision, true); !errors.Is(err, api.ErrLargePayloadReference) {
				t.Fatalf("output collision at threshold %d: %v", threshold, err)
			}
		}
	}
	for _, ordinary := range []string{
		`"blob:v2:literal application string"`,
		`"{\"$durabletaskPayload\":\"literal string\"}"`,
		`{"nested":{"$durabletaskPayload":"ordinary nested object"}}`,
	} {
		wire, err := wrapPayloadReference(ordinary, nil)
		if err != nil || wire != ordinary {
			t.Fatalf("ordinary JSON changed: %q %v", wire, err)
		}
	}
	client := NewClient(&payloadConnection{}, WithClientLargePayloads(options))
	if _, err := client.ScheduleNewOrchestration(t.Context(), "echo",
		api.WithInput(json.RawMessage(envelope))); !errors.Is(err, api.ErrLargePayloadReference) {
		t.Fatalf("management application collision=%v", err)
	}
	disabledConnection := &payloadConnection{}
	disabledClient := NewClient(disabledConnection)
	applicationObject := json.RawMessage(`{"$durabletaskPayload":"application object"}`)
	if _, err := disabledClient.ScheduleNewOrchestration(t.Context(), "echo", api.WithInput(applicationObject)); err != nil {
		t.Fatalf("disabled client changed application object behavior: %v", err)
	}
	if disabledConnection.input != string(applicationObject) {
		t.Fatalf("disabled management input=%q", disabledConnection.input)
	}
	if got, err := transformPayload(t.Context(), nil, string(applicationObject), true); err != nil || got != string(applicationObject) {
		t.Fatalf("disabled execution output=%q error=%v", got, err)
	}
}

type payloadTestStream struct {
	grpc.ClientStream
	sent     proto.Message
	received proto.Message
}

func (s *payloadTestStream) SendMsg(message any) error {
	s.sent = message.(proto.Message)
	return nil
}

func (s *payloadTestStream) RecvMsg(message any) error {
	proto.Merge(message.(proto.Message), s.received)
	return nil
}

func TestFunctionsPayloadStreamingManagement(t *testing.T) {
	token := "blob:v2:https://account.blob.core.windows.net/durabletask-payloads/payload"
	request := newProto(t, "CreateInstanceRequest")
	setStringWrapper(request.ProtoReflect(), "input", token)
	envelope, err := wrapPayloadReference(token, nil)
	if err != nil {
		t.Fatal(err)
	}
	event := newProto(t, "HistoryEvent")
	raw, _ := json.Marshal(map[string]any{
		"eventId": 1, "executionStarted": map[string]any{"name": "echo", "input": envelope},
	})
	if err := protojson.Unmarshal(raw, event); err != nil {
		t.Fatal(err)
	}
	underlying := &payloadTestStream{received: event}
	stream := functionsPayloadStream{ClientStream: underlying}
	if err := stream.SendMsg(request); err != nil {
		t.Fatal(err)
	}
	if got := stringWrapperField(underlying.sent.ProtoReflect(), "input"); got != envelope {
		t.Fatalf("streamed input=%q", got)
	}
	if got := stringWrapperField(request.ProtoReflect(), "input"); got != token {
		t.Fatal("stream send modified caller request")
	}
	result := newProto(t, "HistoryEvent")
	if err := stream.RecvMsg(result); err != nil {
		t.Fatal(err)
	}
	field := result.ProtoReflect().Descriptor().Fields().ByName("executionStarted")
	if got := stringWrapperField(result.ProtoReflect().Get(field).Message(), "input"); got != token {
		t.Fatalf("streamed response=%q", got)
	}
}

type rejectedPayloadConnection struct{ grpc.ClientConnInterface }

func (rejectedPayloadConnection) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return context.Canceled
}

func TestFunctionsPayloadConnectionPreservesRPCFailure(t *testing.T) {
	connection := functionsPayloadConnection{ClientConnInterface: rejectedPayloadConnection{}}
	if err := connection.Invoke(t.Context(), "test", newProto(t, "CreateInstanceRequest"),
		newProto(t, "CreateInstanceResponse")); !errors.Is(err, context.Canceled) {
		t.Fatalf("RPC failure=%v", err)
	}
}

func TestFunctionsPayloadRawManagementCollision(t *testing.T) {
	for _, threshold := range []int{8, 1024} {
		options := memoryPayloadOptions(t)
		options.ThresholdBytes = threshold
		client := NewClient(rejectedPayloadConnection{}, WithClientLargePayloads(options))
		for _, input := range []string{
			`{"$durabletaskPayload":{"version":1,"reference":"blob:v2:https://account.blob.core.windows.net/durabletask-payloads/blob"}}`,
			`{"$durabletaskPayload":"malformed application object"}`,
		} {
			for name, call := range map[string]func() error{
				"schedule": func() error {
					_, err := client.ScheduleNewOrchestration(t.Context(), "echo", api.WithRawInput(input))
					return err
				},
				"event": func() error {
					return client.RaiseEvent(t.Context(), "id", "event", api.WithRawEventData(input))
				},
				"terminate": func() error {
					return client.TerminateOrchestration(t.Context(), "id", api.WithRawOutput(input))
				},
				"signal": func() error {
					return client.SignalEntity(t.Context(), api.NewEntityID("echo", "key"), "echo", api.WithRawSignalInput(input))
				},
			} {
				if err := call(); !errors.Is(err, api.ErrLargePayloadReference) {
					t.Fatalf("%s threshold=%d collision=%s error=%v", name, threshold, input, err)
				}
			}
			// Raw JSON strings containing the reserved key remain user strings.
			literal, _ := json.Marshal(`{"$durabletaskPayload":"literal string"}`)
			if _, err := client.ScheduleNewOrchestration(t.Context(), "echo", api.WithRawInput(string(literal))); err == nil || !strings.Contains(err.Error(), "context canceled") {
				t.Fatalf("literal string rejected before RPC: %v", err)
			}
			disabled := NewClient(rejectedPayloadConnection{})
			if _, err := disabled.ScheduleNewOrchestration(t.Context(), "echo",
				api.WithRawInput(`{"$durabletaskPayload":"application object"}`)); err == nil || !strings.Contains(err.Error(), "context canceled") {
				t.Fatalf("disabled raw object changed: %v", err)
			}
		}
		// Validation observes the final request once, preserving option override
		// semantics rather than rejecting an intermediate value replaced by a caller.
		client = NewClient(rejectedPayloadConnection{}, WithClientLargePayloads(memoryPayloadOptions(t)))
		if _, err := client.ScheduleNewOrchestration(t.Context(), "echo",
			api.WithRawInput(`{"$durabletaskPayload":"intermediate"}`),
			api.WithRawInput(`"`+strings.Repeat("x", 100)+`"`)); err == nil || !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("final raw input override: %v", err)
		}
	}
}
