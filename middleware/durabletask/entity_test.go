package durabletask

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestRunEntityBatch_PreservesOrderedStateAndResults(t *testing.T) {
	d := Middleware()
	d.Entity("counter", func(ctx *EntityContext) (any, error) {
		var increment int
		if err := ctx.GetInput(&increment); err != nil {
			return nil, err
		}
		count := 0
		if ctx.HasState() {
			if err := ctx.GetState(&count); err != nil {
				return nil, err
			}
		}
		count += increment
		if err := ctx.SetState(count); err != nil {
			return nil, err
		}
		return count, nil
	})

	encoded := marshalEntityBatch(t, map[string]any{
		"instanceId":  "@counter@key",
		"entityState": "5",
		"operations": []any{
			map[string]any{"operation": "add", "requestId": "75c3c988-b119-46da-b267-6f1b9d7ac930", "input": "2"},
			map[string]any{"operation": "add", "requestId": "30b57099-4f46-4875-963a-813de87aa6e7", "input": "3"},
		},
	})

	result := runAndDecodeEntityBatch(t, d.registry, encoded)
	if got, want := entityResultStrings(t, result), []string{"7", "10"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("ordered operation results = %v, want %v", got, want)
	}
	if got := entityStringField(t, result, "entityState"); got != "10" {
		t.Fatalf("final entity state = %q, want 10", got)
	}
}

func TestRunEntityBatch_DistinguishesAbsentAndPresentEmptyState(t *testing.T) {
	d := Middleware(WithEntity("presence", func(ctx *EntityContext) (any, error) {
		return ctx.HasState(), nil
	}))

	for _, test := range []struct {
		name        string
		include     bool
		state       string
		wantPresent bool
	}{
		{name: "absent", include: false, wantPresent: false},
		{name: "present empty", include: true, state: "", wantPresent: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := map[string]any{
				"instanceId": "@presence@key",
				"operations": []any{
					map[string]any{"operation": "exists", "requestId": "4a4e598b-7c35-482b-a8a0-64b0a0541dfc"},
				},
			}
			if test.include {
				request["entityState"] = test.state
			}
			result := runAndDecodeEntityBatch(t, d.registry, marshalEntityBatch(t, request))
			if got := entityResultStrings(t, result); len(got) != 1 || got[0] != fmt.Sprint(test.wantPresent) {
				t.Fatalf("HasState result = %v, want %t", got, test.wantPresent)
			}
			if got := entityStringField(t, result, "entityState"); got != test.state {
				t.Fatalf("entity state = %q, want %q", got, test.state)
			}
		})
	}
}

func TestRunEntityBatch_DeletesStateAndRollsBackFailedOperation(t *testing.T) {
	d := Middleware(WithEntity("counter", func(ctx *EntityContext) (any, error) {
		switch ctx.OperationName {
		case "fail":
			if err := ctx.SetState(99); err != nil {
				return nil, err
			}
			return nil, errors.New("expected operation failure")
		case "increment":
			var count int
			if err := ctx.GetState(&count); err != nil {
				return nil, err
			}
			count++
			if err := ctx.SetState(count); err != nil {
				return nil, err
			}
			return count, nil
		case "delete":
			ctx.DeleteState()
			return nil, nil
		default:
			return nil, fmt.Errorf("unexpected operation %q", ctx.OperationName)
		}
	}))

	result := runAndDecodeEntityBatch(t, d.registry, marshalEntityBatch(t, map[string]any{
		"instanceId":  "@counter@key",
		"entityState": "4",
		"operations": []any{
			map[string]any{"operation": "fail", "requestId": "13d9439f-a16b-4ce2-88f2-5ad755d2e06a"},
			map[string]any{"operation": "increment", "requestId": "f069ed40-f5b0-465e-a555-a98691f39a3f"},
		},
	}))
	if got := entityFailureCount(t, result); got != 1 {
		t.Fatalf("failed operation count = %d, want 1", got)
	}
	if got := entityStringField(t, result, "entityState"); got != "5" {
		t.Fatalf("state after rollback and next operation = %q, want 5", got)
	}

	result = runAndDecodeEntityBatch(t, d.registry, marshalEntityBatch(t, map[string]any{
		"instanceId":  "@counter@key",
		"entityState": "5",
		"operations": []any{
			map[string]any{"operation": "delete", "requestId": "116a6b59-1fd4-4b31-b4b8-c7ec18948903"},
		},
	}))
	if hasEntityField(result, "entityState") {
		t.Fatalf("deleted state is still present: %v", result)
	}
}

func TestRunEntityBatch_EmitsImmediateAndScheduledSignals(t *testing.T) {
	scheduled := time.Date(2027, time.January, 2, 3, 4, 5, 0, time.UTC)
	d := Middleware(WithEntity("counter", func(ctx *EntityContext) (any, error) {
		id := api.NewEntityID("counter", "destination")
		if err := ctx.SignalEntity(id, "immediate", 1); err != nil {
			return nil, err
		}
		if err := ctx.SignalEntityAt(id, scheduled, "scheduled", 2); err != nil {
			return nil, err
		}
		return nil, nil
	}))

	result := runAndDecodeEntityBatch(t, d.registry, marshalEntityBatch(t, map[string]any{
		"instanceId": "@counter@source",
		"operations": []any{
			map[string]any{"operation": "emit", "requestId": "9efbc536-1075-4d24-a29c-d1accfbdc683"},
		},
	}))
	actions := entityListField(t, result, "actions")
	if actions.Len() != 2 {
		t.Fatalf("action count = %d, want 2", actions.Len())
	}
	for i := 0; i < actions.Len(); i++ {
		action := actions.Get(i).Message()
		oneof := action.Descriptor().Oneofs().ByName("operationActionType")
		if oneof == nil || action.WhichOneof(oneof).Name() != "sendSignal" {
			t.Fatalf("action %d is not a signal: %v", i, action)
		}
	}
}

func TestRunEntityBatch_RejectsMalformedAndUnsupportedRequests(t *testing.T) {
	d := Middleware(WithEntity("counter", func(*EntityContext) (any, error) { return nil, nil }))
	for _, test := range []struct {
		name    string
		encoded string
		want    string
	}{
		{name: "invalid base64", encoded: "!", want: "base64"},
		{name: "invalid protobuf", encoded: base64.StdEncoding.EncodeToString([]byte{0xff}), want: "parse Functions entity batch request"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := runEntityBatch(context.Background(), d.registry, test.encoded); err == nil || !contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}

	request := map[string]any{
		"instanceId": "@counter@key",
		"properties": map[string]any{"RollbackEntityOperationsOnExceptions": false},
	}
	if _, err := runEntityBatch(context.Background(), d.registry, marshalEntityBatch(t, request)); err == nil || !contains(err.Error(), "does not support") {
		t.Fatalf("rollback=false error = %v, want explicit unsupported error", err)
	}

	request["properties"] = map[string]any{"IncludeState": false}
	result := runAndDecodeEntityBatch(t, d.registry, marshalEntityBatch(t, request))
	if !entityBoolField(t, result, "requiresState") {
		t.Fatal("IncludeState=false must return RequiresState")
	}
}

func TestEntityFactoryProvidesBatchMetadataAndCleanup(t *testing.T) {
	var positions []int
	var size int
	closed := false
	d := Middleware(WithEntityFactory("counter", func(factoryCtx EntityFactoryContext) (EntityBatch, error) {
		if factoryCtx.ID.Name != "counter" || factoryCtx.ID.Key != "key" {
			t.Fatalf("factory entity ID = %v", factoryCtx.ID)
		}
		return EntityBatch{
			Handler: func(ctx *EntityContext) (any, error) {
				positions = append(positions, ctx.BatchPosition)
				size = ctx.BatchSize
				return nil, nil
			},
			Close: func(context.Context) error {
				closed = true
				return nil
			},
		}, nil
	}))

	encoded := marshalEntityBatch(t, map[string]any{
		"instanceId": "@counter@key",
		"operations": []any{
			map[string]any{"operation": "one", "requestId": "9f9b09b2-adf8-4ca4-8a80-443574cb04ac"},
			map[string]any{"operation": "two", "requestId": "e84081e1-b653-48ed-af1c-9aac92acbba0"},
		},
	})
	runAndDecodeEntityBatch(t, d.registry, encoded)
	if fmt.Sprint(positions) != "[0 1]" || size != 2 || !closed {
		t.Fatalf("factory metadata positions=%v size=%d closed=%t", positions, size, closed)
	}
}

func marshalEntityBatch(t *testing.T, request map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	message := newProto(t, "EntityBatchRequest")
	if err := protojson.Unmarshal(raw, message); err != nil {
		t.Fatalf("build entity batch: %v", err)
	}
	encoded, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(encoded)
}

func runAndDecodeEntityBatch(t *testing.T, registry *task.TaskRegistry, encoded string) protoreflect.Message {
	t.Helper()
	result, err := runEntityBatch(context.Background(), registry, encoded)
	if err != nil {
		t.Fatalf("run entity batch: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(result)
	if err != nil {
		t.Fatal(err)
	}
	message := newProto(t, "EntityBatchResult")
	if err := proto.Unmarshal(raw, message); err != nil {
		t.Fatalf("decode entity result: %v", err)
	}
	return message.ProtoReflect()
}

func entityResultStrings(t *testing.T, result protoreflect.Message) []string {
	t.Helper()
	results := entityListField(t, result, "results")
	values := make([]string, results.Len())
	for i := 0; i < results.Len(); i++ {
		success := results.Get(i).Message().Get(
			results.Get(i).Message().Descriptor().Fields().ByName("success"),
		).Message()
		wrapper := success.Get(success.Descriptor().Fields().ByName("result")).Message()
		values[i] = wrapper.Get(wrapper.Descriptor().Fields().ByName("value")).String()
	}
	return values
}

func entityFailureCount(t *testing.T, result protoreflect.Message) int {
	t.Helper()
	results := entityListField(t, result, "results")
	count := 0
	for i := 0; i < results.Len(); i++ {
		oneof := results.Get(i).Message().Descriptor().Oneofs().ByName("resultType")
		if results.Get(i).Message().WhichOneof(oneof).Name() == "failure" {
			count++
		}
	}
	return count
}

func entityStringField(t *testing.T, message protoreflect.Message, name protoreflect.Name) string {
	t.Helper()
	field := message.Descriptor().Fields().ByName(name)
	if field == nil || !message.Has(field) {
		return ""
	}
	return message.Get(field).Message().Get(
		message.Get(field).Message().Descriptor().Fields().ByName("value"),
	).String()
}

func entityBoolField(t *testing.T, message protoreflect.Message, name protoreflect.Name) bool {
	t.Helper()
	field := message.Descriptor().Fields().ByName(name)
	return field != nil && message.Get(field).Bool()
}

func entityListField(t *testing.T, message protoreflect.Message, name protoreflect.Name) protoreflect.List {
	t.Helper()
	field := message.Descriptor().Fields().ByName(name)
	if field == nil || !field.IsList() {
		t.Fatalf("protobuf field %q is not a list", name)
	}
	return message.Get(field).List()
}

func hasEntityField(message protoreflect.Message, name protoreflect.Name) bool {
	field := message.Descriptor().Fields().ByName(name)
	return field != nil && message.Has(field)
}

func contains(value, part string) bool {
	return strings.Contains(value, part)
}
