package durabletask

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/payload"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func memoryPayloadOptions(t *testing.T) *api.LargePayloadOptions {
	t.Helper()
	store := payload.NewMemoryStore()
	options, err := api.NormalizeLargePayloadOptions(&api.LargePayloadOptions{
		Store: store, Resolver: store, ThresholdBytes: 8, MaxPayloadBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	return options
}

func isFunctionsPayloadReference(t *testing.T, value string) bool {
	t.Helper()
	token, err := unwrapPayloadReference(value, nil)
	if err != nil {
		t.Fatal(err)
	}
	return api.IsLargePayloadReference(token)
}

type inclusiveMemoryStore struct{ *payload.MemoryStore }

func (inclusiveMemoryStore) UsesInclusiveLargePayloadThreshold() bool { return true }

func TestLargePayloadThresholdsAndCaps(t *testing.T) {
	for _, inclusive := range []bool{false, true} {
		t.Run(fmt.Sprintf("inclusive=%t", inclusive), func(t *testing.T) {
			options := memoryPayloadOptions(t)
			if inclusive {
				options.Store = inclusiveMemoryStore{payload.NewMemoryStore()}
				options.Resolver = options.Store.(api.LargePayloadResolver)
			}
			for _, size := range []int{7, 8, 9, 1024, 1025} {
				value := strings.Repeat("x", size)
				got, err := transformPayload(t.Context(), options, value, true)
				if size > 1024 {
					if !errors.Is(err, api.ErrLargePayloadTooLarge) {
						t.Fatalf("size %d: error=%v", size, err)
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				wantReference := size > 8 || (inclusive && size == 8)
				if api.IsLargePayloadReference(got) != wantReference {
					t.Fatalf("size %d: reference=%t, want %t", size, api.IsLargePayloadReference(got), wantReference)
				}
				hydrated, err := transformPayload(t.Context(), options, got, false)
				if err != nil || hydrated != value {
					t.Fatalf("size %d: roundtrip=%q error=%v", size, hydrated, err)
				}
				again, err := transformPayload(t.Context(), options, got, true)
				if err != nil || again != got {
					t.Fatalf("size %d: existing reference changed: %v", size, err)
				}
			}
		})
	}
}

func TestLargePayloadDisabledAndApplicationValues(t *testing.T) {
	for _, value := range []string{"", "null", `""`, `"blob:v2:not-a-token"`, `{"location":"blob:v2:x"}`, strings.Repeat("a", 2048)} {
		for _, externalize := range []bool{false, true} {
			got, err := transformPayload(t.Context(), nil, value, externalize)
			if err != nil || got != value {
				t.Fatalf("disabled %q: got %q error %v", value, got, err)
			}
		}
	}
	for _, value := range []string{"blob:v1:bad", "blob:v2:bad", "durabletask-payload:v1:bad"} {
		for _, externalize := range []bool{false, true} {
			if _, err := transformPayload(t.Context(), nil, value, externalize); !errors.Is(err, api.ErrFeatureNotSupported) {
				t.Fatalf("disabled reference %q: %v", value, err)
			}
		}
	}
	options := memoryPayloadOptions(t)
	for _, value := range []string{`"blob:v2:literal"`, `{"nested":"blob:v2:literal"}`, "null", `""`} {
		token, err := transformPayload(t.Context(), options, value, true)
		if err != nil {
			t.Fatal(err)
		}
		got, err := transformPayload(t.Context(), options, token, false)
		if err != nil || got != value {
			t.Fatalf("application value %q: got %q error %v", value, got, err)
		}
	}
}

type failingPayloadStore struct {
	storeErr   error
	resolveErr error
	resolved   []byte
}

func (s failingPayloadStore) Store(context.Context, []byte) (string, error) { return "", s.storeErr }
func (s failingPayloadStore) Resolve(context.Context, string) ([]byte, error) {
	return s.resolved, s.resolveErr
}

func TestLargePayloadReferenceFailuresAndCancellation(t *testing.T) {
	options := memoryPayloadOptions(t)
	token, err := transformPayload(t.Context(), options, `"a long application value"`, true)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, externalize := range []bool{false, true} {
		if _, err := transformPayload(ctx, options, token, externalize); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation: %v", err)
		}
	}
	for _, value := range []string{
		"durabletask-payload:v1:!",
		"durabletask-payload:v1:" + base64.RawURLEncoding.EncodeToString([]byte("{")),
		referenceToken(t, payloadReference{"", 1, strings.Repeat("0", 64)}),
		referenceToken(t, payloadReference{"missing", -1, strings.Repeat("0", 64)}),
		referenceToken(t, payloadReference{"missing", 1025, strings.Repeat("0", 64)}),
		referenceToken(t, payloadReference{"missing", 1, strings.Repeat("z", 64)}),
	} {
		for _, externalize := range []bool{false, true} {
			if _, err := transformPayload(t.Context(), options, value, externalize); !errors.Is(err, api.ErrLargePayloadReference) {
				t.Fatalf("malformed %q: %v", value, err)
			}
		}
	}
	options.Resolver = payload.NewMemoryStore()
	if _, err := transformPayload(t.Context(), options, token, false); err == nil {
		t.Fatal("missing payload accepted")
	}
	sentinel := errors.New("storage unavailable")
	options.Store = failingPayloadStore{storeErr: sentinel}
	if _, err := transformPayload(t.Context(), options, "large payload", true); !errors.Is(err, sentinel) {
		t.Fatalf("store error lost: %v", err)
	}
	options.Store = failingPayloadStore{}
	if _, err := transformPayload(t.Context(), options, "large payload", true); err == nil {
		t.Fatal("empty location accepted")
	}
	options.Resolver = failingPayloadStore{resolveErr: sentinel}
	if _, err := transformPayload(t.Context(), options, token, false); !errors.Is(err, sentinel) {
		t.Fatalf("resolver error lost: %v", err)
	}
	for _, bytes := range [][]byte{[]byte("wrong size"), []byte(`"a long application VALUE"`)} {
		options.Resolver = failingPayloadStore{resolved: bytes}
		if _, err := transformPayload(t.Context(), options, token, false); !errors.Is(err, api.ErrLargePayloadIntegrity) {
			t.Fatalf("integrity failure not detected: %v", err)
		}
	}
}

func referenceToken(t *testing.T, ref payloadReference) string {
	t.Helper()
	descriptor, err := json.Marshal(ref)
	if err != nil {
		t.Fatal(err)
	}
	return api.DurableTaskPayloadReferencePrefix + base64.RawURLEncoding.EncodeToString(descriptor)
}

func TestLargePayloadAzureDefaultsAndUntrustedTokens(t *testing.T) {
	store, err := payload.NewAzureBlobStore(payload.AzureBlobStoreOptions{
		AccountURL: "https://account.blob.core.windows.net", Credential: unusedCredential{},
	})
	if err != nil {
		t.Fatal(err)
	}
	d := Middleware(WithLargePayloads(&api.LargePayloadOptions{Store: store, Resolver: store}))
	if d.largePayloads.ThresholdBytes != 256*1024 || d.largePayloads.MaxPayloadBytes != 10*1024*1024 ||
		d.largePayloads.TokenStore != store {
		t.Fatalf("wrong Azure defaults: %+v", d.largePayloads)
	}
	for _, token := range []string{
		"blob:v2:not-a-url", "blob:v1:invalid",
		"blob:v2:https://evil.example/durabletask-payloads/x",
		"blob:v2:https://account.blob.core.windows.net/other-container/x",
		"blob:v2:https://account.blob.core.windows.net/durabletask-payloads",
		"blob:v2:http://account.blob.core.windows.net/durabletask-payloads/x",
	} {
		for _, externalize := range []bool{false, true} {
			if _, err := transformPayload(t.Context(), d.largePayloads, token, externalize); err == nil {
				t.Fatalf("unsafe token accepted: %q", token)
			}
		}
	}
	value := strings.Repeat("x", 256*1024-1)
	if got, err := transformPayload(t.Context(), d.largePayloads, value, true); err != nil || got != value {
		t.Fatalf("below threshold made a storage request: %v", err)
	}
	if _, err := transformPayload(t.Context(), d.largePayloads, strings.Repeat("x", 10*1024*1024+1), true); !errors.Is(err, api.ErrLargePayloadTooLarge) {
		t.Fatalf("Azure maximum: %v", err)
	}
}

func TestLargePayloadProtoFieldInventory(t *testing.T) {
	options := memoryPayloadOptions(t)
	// Resolve each registered field against upstream descriptors, not a copy of
	// its proto. This fails on removal, renaming or changed wrapper types.
	for messageName, fields := range payloadFields {
		t.Run(string(messageName), func(t *testing.T) {
			message := newProto(t, protoreflect.FullName(messageName)).ProtoReflect()
			for _, field := range fields {
				setStringWrapper(message, field, `"application payload"`)
			}
			if err := transformPayloadMessage(t.Context(), options, message, true); err != nil {
				t.Fatal(err)
			}
			for _, field := range fields {
				if !isFunctionsPayloadReference(t, stringWrapperField(message, field)) {
					t.Fatalf("%s not externalized", field)
				}
			}
			if err := transformPayloadMessage(t.Context(), options, message, false); err != nil {
				t.Fatal(err)
			}
			for _, field := range fields {
				if got := stringWrapperField(message, field); got != `"application payload"` {
					t.Fatalf("%s hydrated to %q", field, got)
				}
			}
		})
	}
}

func TestLargePayloadActivityAdapter(t *testing.T) {
	options := memoryPayloadOptions(t)
	d := Middleware(WithLargePayloads(options))
	for _, value := range []string{"", "blob:v2:literal application string", strings.Repeat("a", 100)} {
		serialized, _ := json.Marshal(value)
		token, err := transformPayload(t.Context(), options, string(serialized), true)
		if err != nil {
			t.Fatal(err)
		}
		called := false
		adapter := d.activityInputDecoder(func(_ context.Context, input string) (string, error) {
			called = true
			if input != value {
				t.Fatalf("input=%q, want %q", input, value)
			}
			return input, nil
		})
		out, err := adapter(t.Context(), []byte(token))
		if err != nil || !called {
			t.Fatalf("activity: %v called=%t", err, called)
		}
		if token == string(serialized) {
			if got, _ := json.Marshal(out); string(got) != string(serialized) {
				t.Fatalf("inline output=%s", got)
			}
		} else {
			wire, _ := json.Marshal(out)
			raw, err := transformPayload(t.Context(), options, string(wire), false)
			if err != nil || raw != string(serialized) {
				t.Fatalf("offloaded output=%q error=%v", raw, err)
			}
		}
	}
	sentinel := errors.New("activity failed")
	adapter := d.activityInputDecoder(func(context.Context) (string, error) {
		return strings.Repeat("x", 2000), sentinel
	})
	if _, err := adapter(t.Context(), nil); !errors.Is(err, sentinel) {
		t.Fatalf("activity failure was replaced by size error: %v", err)
	}
	for _, value := range []any{
		map[string]any{"body": strings.Repeat("x", 100)},
		json.RawMessage(`{"body":"large raw JSON value"}`),
		[]byte("large binary application value"),
		(*string)(nil),
	} {
		adapter := d.activityInputDecoder(func(context.Context) (any, error) { return value, nil })
		out, err := adapter(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		expected, _ := json.Marshal(value)
		wire, _ := json.Marshal(out)
		if isFunctionsPayloadReference(t, string(wire)) {
			got, err := transformPayload(t.Context(), options, string(wire), false)
			if err != nil || got != string(expected) {
				t.Fatalf("output %T = %q, want %q, error %v", value, got, expected, err)
			}
		} else if raw, _ := json.Marshal(out); string(raw) != string(expected) {
			t.Fatalf("inline output %T = %s, want %s", value, raw, expected)
		}
	}
}

type countingPayloadMarshaler struct{ calls *int }

func (m countingPayloadMarshaler) MarshalJSON() ([]byte, error) {
	*m.calls++
	return []byte(fmt.Sprintf("%d", *m.calls)), nil
}

func TestLargePayloadInlineActivitySerializesOnce(t *testing.T) {
	calls := 0
	options := memoryPayloadOptions(t)
	options.ThresholdBytes = 128
	d := Middleware(WithLargePayloads(options))
	adapter := d.activityInputDecoder(func(context.Context) (any, error) {
		return map[string]any{"nested": countingPayloadMarshaler{&calls}}, nil
	})
	output, err := adapter(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(output)
	if err != nil || string(wire) != `{"nested":1}` || calls != 1 {
		t.Fatalf("inline serialization=%s calls=%d error=%v", wire, calls, err)
	}
}

func TestLargePayloadActivityReplacementAndFailures(t *testing.T) {
	options := memoryPayloadOptions(t)
	d := Middleware(WithLargePayloads(options))
	calls := 0
	adapter := d.activityInputDecoder(func(_ context.Context, input string) error {
		calls++
		if input != "replacement" {
			t.Fatalf("input=%q", input)
		}
		return nil
	})
	mc := &sdk.MiddlewareContext{}
	mc.SetInputString(`"replacement"`)
	ctx := sdk.ContextWithMiddleware(t.Context(), mc)
	if _, err := adapter(ctx, []byte(`"original"`)); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter(t.Context(), []byte("durabletask-payload:v1:bad!")); !errors.Is(err, api.ErrLargePayloadReference) {
		t.Fatalf("input reference failure=%v", err)
	}
	if calls != 1 {
		t.Fatalf("bad input invoked application code, calls=%d", calls)
	}
}

func TestLargePayloadProtobufPreservesPresenceAndIdentity(t *testing.T) {
	options := memoryPayloadOptions(t)
	for _, state := range []string{`{}`, `{"entityState":""}`, `{"entityState":"null"}`} {
		message := newProto(t, "EntityBatchRequest")
		if err := protojson.Unmarshal([]byte(state), message); err != nil {
			t.Fatal(err)
		}
		before := proto.Clone(message)
		if err := transformPayloadMessage(t.Context(), options, message.ProtoReflect(), false); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(message, before) {
			t.Fatalf("state presence changed for %s", state)
		}
	}
	message := newProto(t, "OrchestratorResponse")
	if err := protojson.Unmarshal([]byte(`{
		"customStatus":"large custom status",
		"actions":[
			{"id":1,"scheduleTask":{"name":"blob:v2:identity","input":"large activity input"}},
			{"id":2,"completeOrchestration":{"result":"large completion result","carryoverEvents":[
				{"eventId":3,"eventRaised":{"name":"blob:v2:event-name","input":"large carryover input"}}
			]}}
		]
	}`), message); err != nil {
		t.Fatal(err)
	}
	before := proto.Clone(message)
	if err := transformPayloadMessage(t.Context(), options, message.ProtoReflect(), true); err != nil {
		t.Fatal(err)
	}
	wire, err := protojson.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wire), "large activity input") || strings.Contains(string(wire), "large carryover input") {
		t.Fatal("nested payload remained inline")
	}
	if err := transformPayloadMessage(t.Context(), options, message.ProtoReflect(), false); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(message, before) {
		t.Fatal("payload roundtrip changed identity or metadata")
	}
}

func TestLargePayloadOrchestrationAndEntityAdapters(t *testing.T) {
	options := memoryPayloadOptions(t)
	d := Middleware(WithLargePayloads(options))
	d.Orchestrator("echo", func(ctx *task.OrchestrationContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input, nil
	})
	value := `"transparent application value"`
	token, err := transformPayload(t.Context(), options, value, true)
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.StdEncoding.EncodeToString(encodeOrchestratorRequest("instance", nil, [][]byte{
		orchestratorStartedEvent(t), executionStartedEvent(t, "instance", "echo", token),
	}))
	result, err := d.runner.loadAndRun(t.Context(), encoded)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := base64.StdEncoding.DecodeString(result)
	response := newProto(t, "OrchestratorResponse")
	if err := proto.Unmarshal(raw, response); err != nil {
		t.Fatal(err)
	}
	if err := transformPayloadMessage(t.Context(), options, response.ProtoReflect(), false); err != nil {
		t.Fatal(err)
	}
	actions := entityListField(t, response.ProtoReflect(), "actions")
	completion := actions.Get(0).Message()
	completion = completion.Get(completion.Descriptor().Fields().ByName("completeOrchestration")).Message()
	if got := stringWrapperField(completion, "result"); got != value {
		t.Fatalf("orchestration output=%q want %q", got, value)
	}

	d.Entity("echo", func(ctx *EntityContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		if ctx.HasState() {
			var state string
			if err := ctx.GetState(&state); err != nil {
				return nil, err
			}
			if state != input {
				return nil, fmt.Errorf("state %q differs from input %q", state, input)
			}
		}
		if err := ctx.SetState(input); err != nil {
			return nil, err
		}
		return input, nil
	})
	state := token
	for turn := 0; turn < 2; turn++ {
		encoded := marshalEntityBatch(t, map[string]any{
			"instanceId": "@echo@key", "entityState": state,
			"operations": []any{map[string]any{"operation": "echo", "requestId": "75c3c988-b119-46da-b267-6f1b9d7ac930", "input": token}},
		})
		result, err := runEntityBatch(t.Context(), d.registry, encoded, options)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := base64.StdEncoding.DecodeString(result)
		message := newProto(t, "EntityBatchResult")
		if err := proto.Unmarshal(raw, message); err != nil {
			t.Fatal(err)
		}
		state = stringWrapperField(message.ProtoReflect(), "entityState")
		if !isFunctionsPayloadReference(t, state) {
			t.Fatal("entity state was not offloaded")
		}
		if err := transformPayloadMessage(t.Context(), options, message.ProtoReflect(), false); err != nil {
			t.Fatal(err)
		}
		if got := entityResultStrings(t, message.ProtoReflect()); len(got) != 1 || got[0] != value {
			t.Fatalf("turn %d entity result=%v", turn, got)
		}
	}
}

func TestLargePayloadLegacyEntityReplay(t *testing.T) {
	options := memoryPayloadOptions(t)
	d := Middleware(WithLargePayloads(options), WithOrchestrator("CallEntity", func(ctx *task.OrchestrationContext) (any, error) {
		var result string
		err := ctx.CallEntity(api.NewEntityID("echo", "key"), "echo",
			task.WithEntityInput("large entity input")).Await(&result)
		return result, err
	}))
	instanceID := "entity-payload-replay"
	started := orchestratorStartedEvent(t)
	start := executionStartedEvent(t, instanceID, "CallEntity", `""`)
	first, err := d.runner.loadAndRun(t.Context(), base64.StdEncoding.EncodeToString(
		withEntityParameters(t, encodeOrchestratorRequest(instanceID, nil, [][]byte{started, start})),
	))
	if err != nil {
		t.Fatal(err)
	}
	actionID, requestID, targetID, operation, input := findEntityAction(t,
		decodeRunnerResponse(t, first), "entityOperationCalled")
	if !isFunctionsPayloadReference(t, input) {
		t.Fatal("entity call input was not offloaded")
	}
	call, _ := json.Marshal(map[string]any{
		"op": operation, "signal": false, "input": input, "id": requestID,
		"parent": instanceID, "parentExecution": "execution-id",
	})
	sent := marshalHistoryEvent(t, map[string]any{
		"eventId":   actionID,
		"eventSent": map[string]any{"instanceId": targetID, "name": "op", "input": string(call)},
	})
	resultToken, err := transformPayload(t.Context(), options, `"large entity result"`, true)
	if err != nil {
		t.Fatal(err)
	}
	response, _ := json.Marshal(map[string]any{"result": resultToken})
	raised := marshalHistoryEvent(t, map[string]any{
		"eventRaised": map[string]any{"name": requestID, "input": string(response)},
	})
	request := base64.StdEncoding.EncodeToString(withEntityParameters(t,
		encodeOrchestratorRequest(instanceID, [][]byte{started, start, sent},
			[][]byte{orchestratorStartedEvent(t), raised}),
	))
	for turn := 0; turn < 2; turn++ {
		encoded, err := d.runner.loadAndRun(t.Context(), request)
		if err != nil {
			t.Fatalf("replay %d: %v", turn, err)
		}
		result := decodeRunnerResponse(t, encoded)
		complete := findOrchestrationCompletion(t, result)
		token := historyStringWrapper(complete, "result")
		value, err := transformPayload(t.Context(), options, token, false)
		if err != nil || value != `"large entity result"` {
			t.Fatalf("replay %d output=%q error=%v", turn, value, err)
		}
	}
}

type payloadConnection struct{ input string }

func (c *payloadConnection) Invoke(_ context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	if strings.HasSuffix(method, "/GetInstance") {
		raw, err := json.Marshal(map[string]any{
			"exists": true,
			"orchestrationState": map[string]any{
				"instanceId": "test-instance", "name": "echo",
				"createdTimestamp": "2026-01-01T00:00:00Z", "lastUpdatedTimestamp": "2026-01-01T00:00:00Z",
				"input": c.input, "output": c.input, "customStatus": c.input,
			},
		})
		if err != nil {
			return err
		}
		return protojson.Unmarshal(raw, reply.(proto.Message))
	}
	message := args.(proto.Message).ProtoReflect()
	c.input = stringWrapperField(message, "input")
	return protojson.Unmarshal([]byte(`{"instanceId":"test-instance"}`), reply.(proto.Message))
}
func (*payloadConnection) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("stream not expected")
}

func TestLargePayloadClientConfigurationAndOwnership(t *testing.T) {
	options := memoryPayloadOptions(t)
	conn := &payloadConnection{}
	client := NewClient(conn, WithClientLargePayloads(options))
	options.ThresholdBytes = 999
	if _, err := client.ScheduleNewOrchestration(t.Context(), "echo", api.WithInput("application payload")); err != nil {
		t.Fatal(err)
	}
	if !isFunctionsPayloadReference(t, conn.input) {
		t.Fatal("explicit client options were not copied or applied")
	}
	decoded, err := transformPayload(t.Context(), client.largePayloads, conn.input, false)
	if err != nil || decoded != `"application payload"` {
		t.Fatalf("Functions adapter could not read upstream reference: %q %v", decoded, err)
	}
	localToken, err := transformPayload(t.Context(), client.largePayloads, `"application payload"`, true)
	upstreamToken, unwrapErr := unwrapPayloadReference(conn.input, nil)
	if err != nil || unwrapErr != nil || localToken != upstreamToken {
		t.Fatalf("Functions reference differs from upstream: local=%q upstream=%q error=%v", localToken, conn.input, err)
	}
	conn.input, err = wrapPayloadReference(localToken, nil)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := client.FetchOrchestrationMetadata(t.Context(), "test-instance", api.WithFetchPayloads(true))
	if err != nil {
		t.Fatal(err)
	}
	if meta.SerializedInput != `"application payload"` || meta.SerializedOutput != `"application payload"` ||
		meta.SerializedCustomStatus != `"application payload"` {
		t.Fatalf("management fields not hydrated: %+v", meta)
	}
	d := Middleware(WithClient(client))
	if d.client != client || d.largePayloads != client.largePayloads || d.ownsClient {
		t.Fatal("middleware failed to adopt explicit client options and ownership")
	}
	disabled := Middleware(WithClient(client), WithLargePayloads(nil))
	if disabled.client == client || disabled.client.largePayloads != nil || client.largePayloads == nil {
		t.Fatal("middleware override mutated the caller's client")
	}
	if _, err := disabled.client.ScheduleNewOrchestration(t.Context(), "echo", api.WithInput("application payload")); err != nil {
		t.Fatal(err)
	}
	if conn.input != `"application payload"` {
		t.Fatalf("disabled client input=%q", conn.input)
	}
	owned, err := Dial("127.0.0.1:1", WithClientLargePayloads(client.largePayloads))
	if err != nil {
		t.Fatal(err)
	}
	defer owned.Close()
	withBorrowed := Middleware(WithClient(owned), WithLargePayloads(client.largePayloads))
	if err := withBorrowed.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if owned.conn.GetState().String() == "SHUTDOWN" {
		t.Fatal("middleware closed caller-owned connection")
	}
	t.Setenv(EnvGrpcEndpoint, "127.0.0.1:2")
	env := Middleware(WithLargePayloads(client.largePayloads))
	defer env.Shutdown(t.Context())
	if !env.ownsClient || env.client.largePayloads.ThresholdBytes != 8 {
		t.Fatal("environment client did not inherit options")
	}
	cached := d.cachedClient("http://127.0.0.1:3/", "", "")
	defer d.Shutdown(t.Context())
	if cached.largePayloads.ThresholdBytes != 8 || d.cachedClient("http://127.0.0.1:3/", "", "") != cached {
		t.Fatal("binding client did not inherit options or was not reused")
	}
}

func TestLargePayloadConfigurationValidationAndRegistrationOrder(t *testing.T) {
	t.Setenv(EnvGrpcEndpoint, "")
	t.Setenv("AzureWebJobsStorage", "storage alone does not enable payloads")
	if d := Middleware(); d.largePayloads != nil {
		t.Fatal("storage setting enabled payloads implicitly")
	}
	for _, construct := range []func(){
		func() { Middleware(WithLargePayloads(&api.LargePayloadOptions{})) },
		func() { NewClient(&payloadConnection{}, WithClientLargePayloads(&api.LargePayloadOptions{})) },
		func() { _, _ = Dial("127.0.0.1:1", WithClientLargePayloads(&api.LargePayloadOptions{})) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("invalid options did not panic during construction")
				}
			}()
			construct()
		}()
	}
	options := memoryPayloadOptions(t)
	for _, reverse := range []bool{false, true} {
		activity := WithActivity("echo", func(_ context.Context, input string) (string, error) { return input, nil })
		configuration := WithLargePayloads(options)
		opts := []Option{activity, configuration}
		if reverse {
			opts = []Option{configuration, activity}
		}
		d := Middleware(opts...)
		token, err := transformPayload(t.Context(), options, `"application input"`, true)
		if err != nil {
			t.Fatal(err)
		}
		adapter := d.ProvidedFunctions()[0].Func.(func(context.Context, []byte) (any, error))
		output, err := adapter(t.Context(), []byte(token))
		if err != nil {
			t.Fatal(err)
		}
		wire, _ := json.Marshal(output)
		value, err := transformPayload(t.Context(), options, string(wire), false)
		if err != nil || value != `"application input"` {
			t.Fatalf("registration order %t: output=%q error=%v", reverse, value, err)
		}
	}
}

type unusedCredential struct{}

func (unusedCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{}, errors.New("unexpected credential request")
}
