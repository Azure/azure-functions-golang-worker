package durabletask

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestOrchestratorEntityCall_ReplaysFunctionsHistoryAndReturnsResult(t *testing.T) {
	d := Middleware(WithOrchestrator("CallEntity", func(ctx *task.OrchestrationContext) (any, error) {
		var result string
		err := ctx.CallEntity(
			api.NewEntityID("counter", "key"),
			"echo",
			task.WithEntityInput("hello"),
		).Await(&result)
		return result, err
	}))

	instanceID := "entity-call-instance"
	start := executionStartedEvent(t, instanceID, "CallEntity", `""`)
	firstStarted := orchestratorStartedEvent(t)
	initial := withEntityParameters(t, encodeOrchestratorRequest(instanceID, nil, [][]byte{
		firstStarted, start,
	}))
	first, err := d.runner.loadAndRun(context.Background(), base64.StdEncoding.EncodeToString(initial))
	if err != nil {
		t.Fatalf("first entity-call turn: %v", err)
	}
	firstResponse := decodeRunnerResponse(t, first)
	callAction, callID, entityID, operation, input := findEntityAction(t, firstResponse, "entityOperationCalled")
	if operation != "echo" || input != `"hello"` {
		t.Fatalf("call action operation=%q input=%q", operation, input)
	}

	callPayload, err := json.Marshal(map[string]any{
		"op":              operation,
		"signal":          false,
		"input":           input,
		"id":              callID,
		"parent":          instanceID,
		"parentExecution": "host-execution-id",
	})
	if err != nil {
		t.Fatal(err)
	}
	sent := marshalHistoryEvent(t, map[string]any{
		"eventId": callAction,
		"eventSent": map[string]any{
			"instanceId": entityID,
			"name":       "op",
			"input":      string(callPayload),
		},
	})
	past := [][]byte{firstStarted, start, sent}
	raised := marshalHistoryEvent(t, map[string]any{
		"eventRaised": map[string]any{
			"name":  callID,
			"input": `{"result":"\"world\""}`,
		},
	})
	secondRequest := withEntityParameters(t, encodeOrchestratorRequest(instanceID, past, [][]byte{
		orchestratorStartedEvent(t), raised,
	}))
	second, err := d.runner.loadAndRun(context.Background(), base64.StdEncoding.EncodeToString(secondRequest))
	if err != nil {
		t.Fatalf("replay Functions entity call: %v", err)
	}
	secondResponse := decodeRunnerResponse(t, second)
	complete := findOrchestrationCompletion(t, secondResponse)
	if got := historyStringWrapper(complete, "result"); got != `"world"` {
		t.Fatalf("orchestration result = %q, want JSON string %q", got, `"world"`)
	}
}

func TestOrchestratorEntity_RejectsPositiveReorderWindow(t *testing.T) {
	d := Middleware(WithOrchestrator("SignalEntity", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.SignalEntity(api.NewEntityID("counter", "key"), "increment")
	}))
	instanceID := "entity-reorder-window-instance"
	start := executionStartedEvent(t, instanceID, "SignalEntity", `""`)
	request := encodeOrchestratorRequest(instanceID, nil, [][]byte{
		orchestratorStartedEvent(t), start,
	})
	parameters := newProto(t, "OrchestratorEntityParameters")
	window := parameters.ProtoReflect().Descriptor().Fields().ByName("entityMessageReorderWindow")
	parameters.ProtoReflect().Set(window, protoreflect.ValueOfMessage(durationpb.New(time.Minute).ProtoReflect()))
	rawParameters, err := proto.Marshal(parameters)
	if err != nil {
		t.Fatal(err)
	}
	request = protowire.AppendBytes(
		protowire.AppendTag(request, fieldEntityParameters, protowire.BytesType),
		rawParameters,
	)

	if _, err := d.runner.loadAndRun(context.Background(), base64.StdEncoding.EncodeToString(request)); err == nil ||
		!strings.Contains(err.Error(), "EntityMessageReorderWindow=0") {
		t.Fatalf("positive entity reorder window error = %v, want explicit unsupported error", err)
	}
}

func TestOrchestratorEntityLock_ReplaysFunctionsGrantAndRelease(t *testing.T) {
	entityID := api.NewEntityID("counter", "key")
	d := Middleware(WithOrchestrator("LockEntity", func(ctx *task.OrchestrationContext) (any, error) {
		release, err := ctx.LockEntities(entityID)
		if err != nil {
			return nil, err
		}
		defer release()
		var result string
		err = ctx.CallEntity(entityID, "read").Await(&result)
		return result, err
	}))

	instanceID := "entity-lock-instance"
	start := executionStartedEvent(t, instanceID, "LockEntity", `""`)
	firstStarted := orchestratorStartedEvent(t)
	firstRequest := withEntityParameters(t, encodeOrchestratorRequest(instanceID, nil, [][]byte{
		firstStarted, start,
	}))
	firstEncoded, err := d.runner.loadAndRun(context.Background(), base64.StdEncoding.EncodeToString(firstRequest))
	if err != nil {
		t.Fatalf("first lock turn: %v", err)
	}
	firstResponse := decodeRunnerResponse(t, firstEncoded)
	lockAction, lockID, lockTarget, lockSet, position, parent := findLockAction(t, firstResponse, "entityLockRequested")
	if len(lockSet) != 1 || lockSet[0] != "@counter@key" || position != 0 || parent != instanceID {
		t.Fatalf("lock request target=%q set=%v position=%d parent=%q", lockTarget, lockSet, position, parent)
	}

	lockPayload, err := json.Marshal(map[string]any{
		"id":      lockID,
		"parent":  instanceID,
		"lockset": []any{map[string]any{"name": "counter", "key": "key"}},
		"pos":     position,
	})
	if err != nil {
		t.Fatal(err)
	}
	lockSent := marshalHistoryEvent(t, map[string]any{
		"eventId": lockAction,
		"eventSent": map[string]any{
			"instanceId": lockTarget,
			"name":       "op",
			"input":      string(lockPayload),
		},
	})
	lockGranted := marshalHistoryEvent(t, map[string]any{
		"eventRaised": map[string]any{
			"name":  lockID,
			"input": `{"result":"Lock Acquisition Completed"}`,
		},
	})
	past := [][]byte{firstStarted, start, lockSent}
	secondStarted := orchestratorStartedEvent(t)
	secondRequest := withEntityParameters(t, encodeOrchestratorRequest(instanceID, past, [][]byte{
		secondStarted, lockGranted,
	}))
	secondEncoded, err := d.runner.loadAndRun(context.Background(), base64.StdEncoding.EncodeToString(secondRequest))
	if err != nil {
		t.Fatalf("replay lock grant: %v", err)
	}
	secondResponse := decodeRunnerResponse(t, secondEncoded)
	callAction, callID, callTarget, operation, input := findEntityAction(t, secondResponse, "entityOperationCalled")
	if operation != "read" || input != "" || callTarget != lockTarget {
		t.Fatalf("locked call action target=%q operation=%q input=%q", callTarget, operation, input)
	}

	callPayload, err := json.Marshal(map[string]any{
		"op":              operation,
		"signal":          false,
		"id":              callID,
		"parent":          instanceID,
		"parentExecution": "host-execution-id",
	})
	if err != nil {
		t.Fatal(err)
	}
	callSent := marshalHistoryEvent(t, map[string]any{
		"eventId": callAction,
		"eventSent": map[string]any{
			"instanceId": callTarget,
			"name":       "op",
			"input":      string(callPayload),
		},
	})
	callCompleted := marshalHistoryEvent(t, map[string]any{
		"eventRaised": map[string]any{
			"name":  callID,
			"input": `{"result":"\"value\""}`,
		},
	})
	finalRequest := withEntityParameters(t, encodeOrchestratorRequest(
		instanceID,
		[][]byte{firstStarted, start, lockSent, secondStarted, lockGranted, callSent},
		[][]byte{orchestratorStartedEvent(t), callCompleted},
	))
	finalEncoded, err := d.runner.loadAndRun(context.Background(), base64.StdEncoding.EncodeToString(finalRequest))
	if err != nil {
		t.Fatalf("replay entity call under lock: %v", err)
	}
	finalResponse := decodeRunnerResponse(t, finalEncoded)
	if got := historyStringWrapper(findOrchestrationCompletion(t, finalResponse), "result"); got != `"value"` {
		t.Fatalf("locked orchestration result = %q, want JSON string %q", got, `"value"`)
	}
	_, unlockID, unlockTarget, _, _, unlockParent := findLockAction(t, finalResponse, "entityUnlockSent")
	if unlockID != lockID || unlockTarget != lockTarget || unlockParent != instanceID {
		t.Fatalf("unlock action ID=%q target=%q parent=%q", unlockID, unlockTarget, unlockParent)
	}
}

func TestNormalizeFunctionsEntityHistory_MapsCallsSignalsAndResults(t *testing.T) {
	callID := "75c3c988-b119-46da-b267-6f1b9d7ac930"
	signalID := "30b57099-4f46-4875-963a-813de87aa6e7"
	call := historyEventFromJSON(t, map[string]any{
		"eventId": 4,
		"eventSent": map[string]any{
			"instanceId": "@counter@key",
			"name":       "op",
			"input":      `{"op":"echo","signal":false,"input":"\"hello\"","id":"` + callID + `","parent":"orchestration","parentExecution":"execution"}`,
		},
	})
	signal := historyEventFromJSON(t, map[string]any{
		"eventId": 5,
		"eventSent": map[string]any{
			"instanceId": "@counter@key",
			"name":       "op@2027-01-02T03:04:05.0000000Z",
			"input":      `{"op":"notify","signal":true,"input":"2","id":"` + signalID + `","due":"2027-01-02T03:04:05Z"}`,
		},
	})
	reply := historyEventFromJSON(t, map[string]any{
		"eventId": 6,
		"eventRaised": map[string]any{
			"name":  callID,
			"input": `{"result":"\"world\""}`,
		},
	})
	past := []protoreflect.Message{call, signal, reply}

	if _, err := normalizeFunctionsEntityHistory(past, nil); err != nil {
		t.Fatalf("normalize entity history: %v", err)
	}

	assertHistoryEventType(t, call, "entityOperationCalled")
	called := historyEventPayload(call, "entityOperationCalled")
	if got := historyString(called, "requestId"); got != callID {
		t.Fatalf("call request ID = %q, want %q", got, callID)
	}
	if got := historyString(called, "operation"); got != "echo" {
		t.Fatalf("call operation = %q, want echo", got)
	}
	if got := historyStringWrapper(called, "input"); got != `"hello"` {
		t.Fatalf("call input = %q, want JSON string %q", got, `"hello"`)
	}
	assertHistoryEventType(t, signal, "entityOperationSignaled")
	signaled := historyEventPayload(signal, "entityOperationSignaled")
	if got := historyString(signaled, "requestId"); got != signalID {
		t.Fatalf("signal request ID = %q, want %q", got, signalID)
	}
	if !signaled.Has(historyField(signaled, "scheduledTime")) {
		t.Fatal("scheduled signal lost its due time")
	}
	assertHistoryEventType(t, reply, "entityOperationCompleted")
	if got := historyStringWrapper(historyEventPayload(reply, "entityOperationCompleted"), "output"); got != `"world"` {
		t.Fatalf("call result = %q, want JSON string %q", got, `"world"`)
	}
}

func TestNormalizeFunctionsEntityHistory_MapsFailuresAndLockLifecycle(t *testing.T) {
	callID := "13d9439f-a16b-4ce2-88f2-5ad755d2e06a"
	lockID := "f069ed40-f5b0-465e-a555-a98691f39a3f"
	unlockID := "116a6b59-1fd4-4b31-b4b8-c7ec18948903"
	events := []protoreflect.Message{
		historyEventFromJSON(t, map[string]any{
			"eventId": 8,
			"eventSent": map[string]any{
				"instanceId": "@counter@key",
				"name":       "op",
				"input":      `{"op":"fail","signal":false,"id":"` + callID + `","parent":"parent","parentExecution":"exec"}`,
			},
		}),
		historyEventFromJSON(t, map[string]any{
			"eventId": 9,
			"eventSent": map[string]any{
				"instanceId": "@counter@second",
				"name":       "op",
				"input":      `{"id":"` + lockID + `","parent":"parent","lockset":[{"name":"counter","key":"key"},{"name":"counter","key":"second"}],"pos":1}`,
			},
		}),
		historyEventFromJSON(t, map[string]any{
			"eventId": 10,
			"eventSent": map[string]any{
				"instanceId": "@counter@key",
				"name":       "release",
				"input":      `{"parent":"parent","id":"` + unlockID + `"}`,
			},
		}),
		historyEventFromJSON(t, map[string]any{
			"eventId": 11,
			"eventRaised": map[string]any{
				"name":  callID,
				"input": `{"result":"{\"Message\":\"operation failed\"}","exceptionType":"System.Exception"}`,
			},
		}),
		historyEventFromJSON(t, map[string]any{
			"eventId": 12,
			"eventRaised": map[string]any{
				"name":  lockID,
				"input": `{"result":"Lock Acquisition Completed"}`,
			},
		}),
	}

	if _, err := normalizeFunctionsEntityHistory(events, nil); err != nil {
		t.Fatalf("normalize entity history: %v", err)
	}
	assertHistoryEventType(t, events[0], "entityOperationCalled")
	assertHistoryEventType(t, events[1], "entityLockRequested")
	lock := historyEventPayload(events[1], "entityLockRequested")
	if got := historyString(lock, "criticalSectionId"); got != lockID {
		t.Fatalf("critical section ID = %q, want %q", got, lockID)
	}
	lockSet := lock.Get(historyField(lock, "lockSet")).List()
	if got, want := lockSet.Len(), 2; got != want {
		t.Fatalf("lock set length = %d, want %d", got, want)
	}
	assertHistoryEventType(t, events[2], "entityUnlockSent")
	assertHistoryEventType(t, events[3], "entityOperationFailed")
	failure := historyEventPayload(events[3], "entityOperationFailed").
		Get(historyField(historyEventPayload(events[3], "entityOperationFailed"), "failureDetails")).Message()
	if got := historyString(failure, "errorMessage"); got != "operation failed" {
		t.Fatalf("failure message = %q, want operation failed", got)
	}
	assertHistoryEventType(t, events[4], "entityLockGranted")
}

func TestNormalizeFunctionsEntityHistory_MapsHostFailureDetails(t *testing.T) {
	callID := "47ed85cc-5f9c-4c62-8c0e-3425a9c44b75"
	events := []protoreflect.Message{
		historyEventFromJSON(t, map[string]any{
			"eventId": 1,
			"eventSent": map[string]any{
				"instanceId": "@counter@key",
				"name":       "op",
				"input":      `{"op":"fail","signal":false,"id":"` + callID + `","parent":"parent","parentExecution":"exec"}`,
			},
		}),
		historyEventFromJSON(t, map[string]any{
			"eventRaised": map[string]any{
				"name": callID,
				"input": `{"result":null,"failureDetails":{"ErrorType":"*errors.errorString",` +
					`"ErrorMessage":"intentional entity failure","StackTrace":null,"InnerFailure":null,` +
					`"IsNonRetriable":false,"Properties":{"source":"entity"}}}`,
			},
		}),
	}

	if _, err := normalizeFunctionsEntityHistory(events, nil); err != nil {
		t.Fatalf("normalize host entity failure: %v", err)
	}
	assertHistoryEventType(t, events[1], "entityOperationFailed")
	failure := historyEventPayload(events[1], "entityOperationFailed").
		Get(historyField(historyEventPayload(events[1], "entityOperationFailed"), "failureDetails")).Message()
	if got := historyString(failure, "errorType"); got != "*errors.errorString" {
		t.Fatalf("failure type = %q, want host error type", got)
	}
	if got := historyString(failure, "errorMessage"); got != "intentional entity failure" {
		t.Fatalf("failure message = %q, want host error message", got)
	}
	properties := failure.Get(historyField(failure, "properties")).Map()
	property := properties.Get(protoreflect.ValueOfString("source").MapKey()).Message()
	stringValue := property.Descriptor().Fields().ByName("string_value")
	if stringValue == nil {
		stringValue = property.Descriptor().Fields().ByName("stringValue")
	}
	if stringValue == nil {
		t.Fatal("failure property protobuf has no string value field")
	}
	if got := property.Get(stringValue).String(); got != "entity" {
		t.Fatalf("failure source property = %q, want entity", got)
	}
}

func TestNormalizeFunctionsEntityHistory_RejectsMalformedEntityMessages(t *testing.T) {
	for _, test := range []struct {
		name  string
		input string
		want  string
	}{
		{name: "invalid request ID", input: `{"op":"increment","signal":false,"id":"not-a-guid"}`, want: "request ID"},
	} {
		t.Run(test.name, func(t *testing.T) {
			event := historyEventFromJSON(t, map[string]any{
				"eventId": 1,
				"eventSent": map[string]any{
					"instanceId": "@counter@key",
					"name":       "op",
					"input":      test.input,
				},
			})
			if _, err := normalizeFunctionsEntityHistory([]protoreflect.Message{event}, nil); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("normalization error = %v, want %q", err, test.want)
			}
		})
	}

	event := historyEventFromJSON(t, map[string]any{
		"eventId": 2,
		"eventSent": map[string]any{
			"instanceId": "@counter@key",
			"name":       "op",
			"input":      `{"op":"increment","id":"13d9439f-a16b-4ce2-88f2-5ad755d2e06a"}`,
		},
	})
	if _, err := normalizeFunctionsEntityHistory([]protoreflect.Message{event}, nil); err != nil {
		t.Fatalf("normalize Functions history without signal metadata: %v", err)
	}
	assertHistoryEventType(t, event, "entityOperationCalled")
}

func historyEventFromJSON(t *testing.T, event map[string]any) protoreflect.Message {
	t.Helper()
	return newHistoryEventMessage(t, marshalHistoryEvent(t, event))
}

func newHistoryEventMessage(t *testing.T, raw []byte) protoreflect.Message {
	t.Helper()
	message := newProto(t, "HistoryEvent")
	if err := proto.Unmarshal(raw, message); err != nil {
		t.Fatalf("unmarshal history event: %v", err)
	}
	return message.ProtoReflect()
}

func assertHistoryEventType(t *testing.T, event protoreflect.Message, want protoreflect.Name) {
	t.Helper()
	oneof := event.Descriptor().Oneofs().ByName("eventType")
	if oneof == nil || event.WhichOneof(oneof) == nil || event.WhichOneof(oneof).Name() != want {
		t.Fatalf("history event type = %v, want %s", event.WhichOneof(oneof), want)
	}
}

func historyEventPayload(event protoreflect.Message, name protoreflect.Name) protoreflect.Message {
	return event.Get(historyField(event, name)).Message()
}

func historyField(message protoreflect.Message, name protoreflect.Name) protoreflect.FieldDescriptor {
	return message.Descriptor().Fields().ByName(name)
}

func historyString(message protoreflect.Message, name protoreflect.Name) string {
	return message.Get(historyField(message, name)).String()
}

func historyStringWrapper(message protoreflect.Message, name protoreflect.Name) string {
	wrapper := message.Get(historyField(message, name)).Message()
	return wrapper.Get(historyField(wrapper, "value")).String()
}

func withEntityParameters(t *testing.T, request []byte) []byte {
	t.Helper()
	parameters := newProto(t, "OrchestratorEntityParameters")
	durationField := parameters.ProtoReflect().Descriptor().Fields().ByName("entityMessageReorderWindow")
	parameters.ProtoReflect().Set(durationField, protoreflect.ValueOfMessage(durationpb.New(0).ProtoReflect()))
	raw, err := proto.Marshal(parameters)
	if err != nil {
		t.Fatal(err)
	}
	return protowire.AppendBytes(protowire.AppendTag(request, 5, protowire.BytesType), raw)
}

func decodeRunnerResponse(t *testing.T, encoded string) protoreflect.Message {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	response := newProto(t, "OrchestratorResponse")
	if err := proto.Unmarshal(raw, response); err != nil {
		t.Fatal(err)
	}
	return response.ProtoReflect()
}

func findEntityAction(t *testing.T, response protoreflect.Message, wantType protoreflect.Name) (
	id int32, requestID, entityID, operation, input string,
) {
	t.Helper()
	actions := response.Get(historyField(response, "actions")).List()
	for i := 0; i < actions.Len(); i++ {
		action := actions.Get(i).Message()
		send := action.Descriptor().Fields().ByName("sendEntityMessage")
		if send == nil || !action.Has(send) {
			continue
		}
		message := action.Get(send).Message()
		selected := message.Descriptor().Fields().ByName(wantType)
		if selected == nil || !message.Has(selected) {
			continue
		}
		payload := message.Get(selected).Message()
		return int32(action.Get(historyField(action, "id")).Int()),
			historyString(payload, "requestId"),
			historyStringWrapper(payload, "targetInstanceId"),
			historyString(payload, "operation"),
			historyStringWrapper(payload, "input")
	}
	t.Fatalf("no %s action in response %v", wantType, response)
	return 0, "", "", "", ""
}

func findLockAction(t *testing.T, response protoreflect.Message, wantType protoreflect.Name) (
	actionID int32,
	criticalSectionID string,
	target string,
	lockSet []string,
	position int32,
	parent string,
) {
	t.Helper()
	actions := response.Get(historyField(response, "actions")).List()
	for i := 0; i < actions.Len(); i++ {
		action := actions.Get(i).Message()
		send := action.Descriptor().Fields().ByName("sendEntityMessage")
		if send == nil || !action.Has(send) {
			continue
		}
		message := action.Get(send).Message()
		selected := message.Descriptor().Fields().ByName(wantType)
		if selected == nil || !message.Has(selected) {
			continue
		}
		payload := message.Get(selected).Message()
		if field := payload.Descriptor().Fields().ByName("lockSet"); field != nil && field.IsList() {
			list := payload.Get(field).List()
			lockSet = make([]string, list.Len())
			for j := range lockSet {
				lockSet[j] = list.Get(j).String()
			}
		}
		if field := payload.Descriptor().Fields().ByName("position"); field != nil {
			position = int32(payload.Get(field).Int())
		}
		if field := payload.Descriptor().Fields().ByName("targetInstanceId"); field != nil {
			if field.Kind() == protoreflect.StringKind {
				target = payload.Get(field).String()
			} else {
				target = historyStringWrapper(payload, "targetInstanceId")
			}
		} else if int(position) < len(lockSet) {
			target = lockSet[position]
		}
		if field := payload.Descriptor().Fields().ByName("parentInstanceId"); field != nil {
			parent = payload.Get(field).Message().Get(
				field.Message().Fields().ByName("value"),
			).String()
		}
		return int32(action.Get(historyField(action, "id")).Int()),
			historyString(payload, "criticalSectionId"),
			target,
			lockSet,
			position,
			parent
	}
	t.Fatalf("no %s action in response %v", wantType, response)
	return 0, "", "", nil, 0, ""
}

func findOrchestrationCompletion(t *testing.T, response protoreflect.Message) protoreflect.Message {
	t.Helper()
	actions := response.Get(historyField(response, "actions")).List()
	for i := 0; i < actions.Len(); i++ {
		action := actions.Get(i).Message()
		field := action.Descriptor().Fields().ByName("completeOrchestration")
		if field != nil && action.Has(field) {
			return action.Get(field).Message()
		}
	}
	t.Fatalf("no orchestration completion action in response %v", response)
	return nil
}
