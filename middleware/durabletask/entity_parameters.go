package durabletask

import (
	"fmt"

	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
)

func normalizeEntityParameters(message proto.Message) (bool, error) {
	parameters := message.ProtoReflect()
	windowField := parameters.Descriptor().Fields().ByName("entityMessageReorderWindow")
	if windowField == nil || windowField.Kind() != protoreflect.MessageKind || !parameters.Has(windowField) {
		return false, nil
	}
	window := parameters.Get(windowField).Message()
	secondsField := window.Descriptor().Fields().ByName("seconds")
	nanosField := window.Descriptor().Fields().ByName("nanos")
	if secondsField == nil || nanosField == nil {
		return false, fmt.Errorf("durabletask: unsupported entity reorder-window protobuf")
	}
	duration := &durationpb.Duration{
		Seconds: window.Get(secondsField).Int(),
		Nanos:   int32(window.Get(nanosField).Int()),
	}
	if err := duration.CheckValid(); err != nil {
		return false, fmt.Errorf("durabletask: invalid entity reorder window: %w", err)
	}
	if duration.Seconds < 0 || duration.Nanos < 0 {
		return false, fmt.Errorf("durabletask: negative entity reorder windows are not supported")
	}
	if duration.Seconds == 0 && duration.Nanos == 0 {
		return false, nil
	}

	parameters.Set(windowField, protoreflect.ValueOfMessage(durationpb.New(0).ProtoReflect()))
	return true, nil
}

func orchestrationHasEntityActions(results *task.ExecutionResults) bool {
	if results == nil || results.Response == nil {
		return false
	}
	response := results.Response.ProtoReflect()
	actionsField := response.Descriptor().Fields().ByName("actions")
	if actionsField == nil || !actionsField.IsList() {
		return false
	}
	actions := response.Get(actionsField).List()
	for i := 0; i < actions.Len(); i++ {
		action := actions.Get(i).Message()
		oneof := action.Descriptor().Oneofs().ByName("orchestratorActionType")
		if oneof == nil {
			continue
		}
		field := action.WhichOneof(oneof)
		if field != nil && field.Name() == "sendEntityMessage" {
			return true
		}
	}
	return false
}
