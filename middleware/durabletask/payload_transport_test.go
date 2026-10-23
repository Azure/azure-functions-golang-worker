package durabletask

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestFunctionsPayloadEnvelopeAndCollisions(t *testing.T) {
	options := memoryPayloadOptions(t)
	value := `"application string"`
	token, err := transformPayload(t.Context(), options, value, true)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := wrapPayloadReference(token)
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
		wire, err := wrapPayloadReference(ordinary)
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
	envelope, err := wrapPayloadReference(token)
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
	connection := functionsPayloadConnection{rejectedPayloadConnection{}}
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
