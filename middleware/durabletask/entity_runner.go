package durabletask

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"
)

func runEntityBatch(ctx context.Context, registry *task.TaskRegistry, encoded string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("durabletask: decode entity batch base64: %w", err)
	}

	executor, ok := task.NewTaskExecutor(registry).(task.EntityExecutor)
	if !ok {
		return "", fmt.Errorf("durabletask: upstream executor does not support entity batches")
	}
	response, err := executeEntityBatch(ctx, executor.ExecuteEntity, raw)
	if err != nil {
		return "", fmt.Errorf("durabletask: execute entity batch: %w", err)
	}
	responseBytes, err := proto.Marshal(response)
	if err != nil {
		return "", fmt.Errorf("durabletask: marshal entity batch response: %w", err)
	}
	return base64.StdEncoding.EncodeToString(responseBytes), nil
}

func executeEntityBatch[Request, Response any, RequestPtr interface {
	*Request
	proto.Message
}, ResponsePtr interface {
	*Response
	proto.Message
}](
	ctx context.Context,
	execute func(context.Context, *Request) (*Response, error),
	raw []byte,
) (ResponsePtr, error) {
	request := RequestPtr(new(Request))
	if err := proto.Unmarshal(raw, request); err != nil {
		var zero ResponsePtr
		return zero, fmt.Errorf("parse Functions entity batch request: %w", err)
	}

	requestMessage := RequestPtr(request).ProtoReflect()
	operationsField := requestMessage.Descriptor().Fields().ByName("operations")
	if operationsField == nil || operationsField.Kind() != protoreflect.MessageKind || !operationsField.IsList() {
		var zero ResponsePtr
		return zero, fmt.Errorf("upstream entity batch request has no operations field")
	}
	ctx = context.WithValue(ctx, entityBatchInfoKey{}, &entityBatchInfo{
		size: requestMessage.Get(operationsField).List().Len(),
	})

	if rollback, ok, err := entityBooleanProperty(requestMessage, "RollbackEntityOperationsOnExceptions"); err != nil {
		var zero ResponsePtr
		return zero, err
	} else if ok && !rollback {
		var zero ResponsePtr
		return zero, fmt.Errorf(
			"Functions entity batch disables RollbackEntityOperationsOnExceptions, which durabletask-go does not support",
		)
	}

	if includeState, ok, err := entityBooleanProperty(requestMessage, "IncludeState"); err != nil {
		var zero ResponsePtr
		return zero, err
	} else if ok && !includeState {
		response := ResponsePtr(new(Response))
		requiresStateField := response.ProtoReflect().Descriptor().Fields().ByName("requiresState")
		if requiresStateField == nil || requiresStateField.Kind() != protoreflect.BoolKind {
			var zero ResponsePtr
			return zero, fmt.Errorf("upstream entity batch response has no requiresState field")
		}
		response.ProtoReflect().Set(requiresStateField, protoreflect.ValueOfBool(true))
		return response, nil
	}

	response, err := execute(ctx, (*Request)(request))
	if err != nil {
		var zero ResponsePtr
		return zero, err
	}
	return ResponsePtr(response), nil
}

func entityBooleanProperty(message protoreflect.Message, name string) (value bool, present bool, err error) {
	propertiesField := message.Descriptor().Fields().ByName("properties")
	if propertiesField == nil || propertiesField.Kind() != protoreflect.MessageKind || !propertiesField.IsMap() {
		return false, false, fmt.Errorf("upstream entity batch request has no properties map")
	}
	properties := message.Get(propertiesField).Map()
	key := protoreflect.ValueOfString(name).MapKey()
	if !properties.Has(key) {
		return false, false, nil
	}
	property, ok := properties.Get(key).Message().Interface().(*structpb.Value)
	if !ok {
		return false, false, fmt.Errorf("Functions entity property %q has an unexpected protobuf type", name)
	}
	if property.GetKind() == nil {
		return false, false, fmt.Errorf("Functions entity property %q has no value", name)
	}
	boolValue, ok := property.GetKind().(*structpb.Value_BoolValue)
	if !ok {
		return false, false, fmt.Errorf("Functions entity property %q is not a boolean", name)
	}
	return boolValue.BoolValue, true, nil
}
