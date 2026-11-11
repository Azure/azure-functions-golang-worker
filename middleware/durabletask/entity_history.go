package durabletask

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type functionsEntityID struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

func (id functionsEntityID) schedulerID() string {
	return "@" + strings.ToLower(id.Name) + "@" + id.Key
}

type functionsEntityRequest struct {
	Operation    *string             `json:"op"`
	IsSignal     *bool               `json:"signal"`
	Input        *string             `json:"input"`
	ID           string              `json:"id"`
	Parent       string              `json:"parent"`
	ParentExec   string              `json:"parentExecution"`
	Due          *time.Time          `json:"due"`
	LockSet      []functionsEntityID `json:"lockset"`
	LockPosition int                 `json:"pos"`
}

type functionsEntityRelease struct {
	Parent string `json:"parent"`
	ID     string `json:"id"`
}

type functionsEntityResponse struct {
	Result         *string                        `json:"result"`
	ExceptionType  *string                        `json:"exceptionType"`
	FailureDetails *functionsEntityFailureDetails `json:"failureDetails"`
}

type functionsEntityFailureDetails struct {
	ErrorType      string                         `json:"errorType"`
	ErrorMessage   string                         `json:"errorMessage"`
	StackTrace     *string                        `json:"stackTrace"`
	InnerFailure   *functionsEntityFailureDetails `json:"innerFailure"`
	IsNonRetriable bool                           `json:"isNonRetriable"`
	Properties     map[string]any                 `json:"properties"`
}

// normalizeFunctionsEntityHistory translates the legacy event format emitted
// by the Functions host into the entity history events the upstream executor
// replays. It changes only entity messages, leaving application events intact.
func normalizeFunctionsEntityHistory(past, next []protoreflect.Message) (bool, error) {
	operationIDs := make(map[string]struct{})
	lockIDs := make(map[string]struct{})
	changed := false

	for _, events := range [][]protoreflect.Message{past, next} {
		for _, event := range events {
			eventMessage := event
			switch eventFieldName(eventMessage) {
			case "eventSent":
				sent := eventMessage.Get(eventField(eventMessage, "eventSent")).Message()
				if !strings.HasPrefix(stringField(sent, "instanceId"), "@") {
					continue
				}
				requestID, isLock, isEntity, err := normalizeFunctionsEntitySend(eventMessage, sent)
				if err != nil {
					return false, err
				}
				if isEntity {
					changed = true
					if isLock {
						lockIDs[requestID] = struct{}{}
					} else {
						operationIDs[requestID] = struct{}{}
					}
				}
			}
		}
	}

	for _, events := range [][]protoreflect.Message{past, next} {
		for _, event := range events {
			eventMessage := event
			switch eventFieldName(eventMessage) {
			case "eventRaised":
				raised := eventMessage.Get(eventField(eventMessage, "eventRaised")).Message()
				name := strings.ToLower(stringField(raised, "name"))
				if _, ok := operationIDs[name]; ok {
					if err := normalizeFunctionsEntityResponse(eventMessage, raised, name); err != nil {
						return false, err
					}
					changed = true
				} else if _, ok := lockIDs[name]; ok {
					if err := normalizeFunctionsEntityLockGrant(eventMessage, name); err != nil {
						return false, err
					}
					changed = true
				}
			}
		}
	}
	return changed, nil
}

func normalizeFunctionsEntitySend(
	event protoreflect.Message,
	sent protoreflect.Message,
) (requestID string, isLock bool, isEntity bool, err error) {
	instanceID := stringField(sent, "instanceId")
	name := stringField(sent, "name")
	data := stringWrapperField(sent, "input")
	switch {
	case name == "release":
		var release functionsEntityRelease
		if err := json.Unmarshal([]byte(data), &release); err != nil {
			return "", false, false, fmt.Errorf("parse Functions entity release message: %w", err)
		}
		requestID, err := parseEntityRequestID(release.ID)
		if err != nil {
			return "", false, false, err
		}
		targetID, err := parseFunctionsEntityID(instanceID)
		if err != nil {
			return "", false, false, err
		}
		replaceEvent(event, "entityUnlockSent", func(message protoreflect.Message) {
			setString(message, "criticalSectionId", requestID)
			setStringWrapper(message, "parentInstanceId", release.Parent)
			setStringWrapper(message, "targetInstanceId", targetID)
		})
		return requestID, false, true, nil

	case name == "op" || strings.HasPrefix(name, "op@"):
		var request functionsEntityRequest
		if err := json.Unmarshal([]byte(data), &request); err != nil {
			return "", false, false, fmt.Errorf("parse Functions entity request message: %w", err)
		}
		requestID, err := parseEntityRequestID(request.ID)
		if err != nil {
			return "", false, false, err
		}

		if request.LockSet != nil {
			if len(request.LockSet) == 0 || request.LockPosition < 0 || request.LockPosition >= len(request.LockSet) {
				return "", false, false, fmt.Errorf("Functions entity lock request has an invalid lock set position")
			}
			replaceEvent(event, "entityLockRequested", func(message protoreflect.Message) {
				setString(message, "criticalSectionId", requestID)
				setStringList(message, "lockSet", entitySchedulerIDs(request.LockSet))
				setInt32(message, "position", int32(request.LockPosition))
				setStringWrapper(message, "parentInstanceId", request.Parent)
			})
			return requestID, true, true, nil
		}
		if request.Operation == nil || *request.Operation == "" {
			return "", false, false, fmt.Errorf("Functions entity operation request has no operation name")
		}
		if strings.HasPrefix(name, "op@") {
			if request.Due == nil {
				return "", false, false, fmt.Errorf("scheduled Functions entity operation has no due time")
			}
			nameTime, err := time.Parse(time.RFC3339Nano, strings.TrimPrefix(name, "op@"))
			if err != nil {
				return "", false, false, fmt.Errorf("parse scheduled Functions entity event name %q: %w", name, err)
			}
			if !request.Due.Equal(nameTime) {
				return "", false, false, fmt.Errorf("Functions entity request due time does not match its scheduled event name")
			}
		} else if request.Due != nil {
			return "", false, false, fmt.Errorf("Functions entity request due time does not match its scheduled event name")
		}

		// Both upstream operation-sent history variants acknowledge a pending
		// entity action by request ID. Functions history omits signal metadata,
		// so use the signaled variant only when the host actually includes it.
		eventName := "entityOperationCalled"
		if request.IsSignal != nil && *request.IsSignal {
			eventName = "entityOperationSignaled"
		}
		targetID, err := parseFunctionsEntityID(instanceID)
		if err != nil {
			return "", false, false, err
		}
		replaceEvent(event, protoreflect.Name(eventName), func(message protoreflect.Message) {
			setString(message, "requestId", requestID)
			setString(message, "operation", *request.Operation)
			setStringWrapperIfPresent(message, "input", request.Input)
			setTimestampIfPresent(message, "scheduledTime", request.Due)
			setStringWrapper(message, "targetInstanceId", targetID)
		})
		return requestID, false, true, nil
	default:
		return "", false, false, nil
	}
}

func normalizeFunctionsEntityResponse(
	event protoreflect.Message,
	raised protoreflect.Message,
	requestID string,
) error {
	var response functionsEntityResponse
	if err := json.Unmarshal([]byte(stringWrapperField(raised, "input")), &response); err != nil {
		return fmt.Errorf("parse Functions entity response %q: %w", requestID, err)
	}
	if response.FailureDetails != nil || response.ExceptionType != nil {
		var conversionErr error
		replaceEvent(event, "entityOperationFailed", func(failureEvent protoreflect.Message) {
			setString(failureEvent, "requestId", requestID)
			failure := mutableMessage(failureEvent, "failureDetails")
			if response.FailureDetails != nil {
				conversionErr = setFunctionsEntityFailureDetails(failure, response.FailureDetails)
			} else {
				setString(failure, "errorType", *response.ExceptionType)
				setString(failure, "errorMessage", functionsEntityErrorMessage(response.Result))
			}
		})
		if conversionErr != nil {
			return fmt.Errorf("convert Functions entity failure details %q: %w", requestID, conversionErr)
		}
		return nil
	}

	replaceEvent(event, "entityOperationCompleted", func(completed protoreflect.Message) {
		setString(completed, "requestId", requestID)
		setStringWrapperIfPresent(completed, "output", response.Result)
	})
	return nil
}

func setFunctionsEntityFailureDetails(target protoreflect.Message, details *functionsEntityFailureDetails) error {
	setString(target, "errorType", details.ErrorType)
	setString(target, "errorMessage", details.ErrorMessage)
	if details.StackTrace != nil {
		setStringWrapper(target, "stackTrace", *details.StackTrace)
	}
	setBool(target, "isNonRetriable", details.IsNonRetriable)
	if details.InnerFailure != nil {
		if err := setFunctionsEntityFailureDetails(mutableMessage(target, "innerFailure"), details.InnerFailure); err != nil {
			return err
		}
	}
	if len(details.Properties) != 0 {
		field := target.Descriptor().Fields().ByName("properties")
		if field == nil || !field.IsMap() || field.MapKey().Kind() != protoreflect.StringKind ||
			field.MapValue().Kind() != protoreflect.MessageKind ||
			field.MapValue().Message().FullName() != "google.protobuf.Value" {
			panic("durabletask: upstream failure details properties are not a string-keyed protobuf Value map")
		}
		properties := target.Mutable(field).Map()
		for key, value := range details.Properties {
			encoded, err := structpb.NewValue(value)
			if err != nil {
				return fmt.Errorf("encode property %q: %w", key, err)
			}
			properties.Set(protoreflect.ValueOfString(key).MapKey(), protoreflect.ValueOfMessage(encoded.ProtoReflect()))
		}
	}
	return nil
}

func normalizeFunctionsEntityLockGrant(event protoreflect.Message, criticalSectionID string) error {
	replaceEvent(event, "entityLockGranted", func(message protoreflect.Message) {
		setString(message, "criticalSectionId", criticalSectionID)
	})
	return nil
}

func functionsEntityErrorMessage(result *string) string {
	if result == nil || *result == "" {
		return "Functions entity operation failed"
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal([]byte(*result), &payload); err == nil {
		for key, raw := range payload {
			if strings.EqualFold(key, "Message") {
				var message string
				if json.Unmarshal(raw, &message) == nil && message != "" {
					return message
				}
			}
		}
	}
	return *result
}

func parseEntityRequestID(value string) (string, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return "", fmt.Errorf("parse Functions entity request ID %q: %w", value, err)
	}
	return id.String(), nil
}

func parseFunctionsEntityID(value string) (string, error) {
	if !strings.HasPrefix(value, "@") {
		return "", fmt.Errorf("invalid Functions entity ID %q", value)
	}
	separator := strings.Index(value[1:], "@")
	if separator < 1 || separator == len(value[1:])-1 {
		return "", fmt.Errorf("invalid Functions entity ID %q", value)
	}
	return value, nil
}

func entitySchedulerIDs(ids []functionsEntityID) []string {
	result := make([]string, len(ids))
	for i, id := range ids {
		result[i] = id.schedulerID()
	}
	return result
}

func eventFieldName(event protoreflect.Message) protoreflect.Name {
	oneof := event.Descriptor().Oneofs().ByName("eventType")
	if oneof == nil {
		return ""
	}
	field := event.WhichOneof(oneof)
	if field == nil {
		return ""
	}
	return field.Name()
}

func eventField(event protoreflect.Message, name protoreflect.Name) protoreflect.FieldDescriptor {
	field := event.Descriptor().Fields().ByName(name)
	if field == nil {
		panic(fmt.Sprintf("durabletask: upstream history event has no %q field", name))
	}
	return field
}

func replaceEvent(event protoreflect.Message, name protoreflect.Name, fill func(protoreflect.Message)) {
	oneof := event.Descriptor().Oneofs().ByName("eventType")
	field := event.Descriptor().Fields().ByName(name)
	if oneof == nil || field == nil || field.Kind() != protoreflect.MessageKind {
		panic(fmt.Sprintf("durabletask: upstream history event has no entity event %q", name))
	}
	message := event.NewField(field).Message()
	fill(message)
	event.Clear(event.WhichOneof(oneof))
	event.Set(field, protoreflect.ValueOfMessage(message))
}

func mutableMessage(message protoreflect.Message, name protoreflect.Name) protoreflect.Message {
	field := message.Descriptor().Fields().ByName(name)
	if field == nil || field.Kind() != protoreflect.MessageKind {
		panic(fmt.Sprintf("durabletask: upstream protobuf has no message field %q", name))
	}
	return message.Mutable(field).Message()
}

func stringField(message protoreflect.Message, name protoreflect.Name) string {
	field := message.Descriptor().Fields().ByName(name)
	if field == nil || field.Kind() != protoreflect.StringKind {
		panic(fmt.Sprintf("durabletask: upstream protobuf has no string field %q", name))
	}
	return message.Get(field).String()
}

func stringWrapperField(message protoreflect.Message, name protoreflect.Name) string {
	field := message.Descriptor().Fields().ByName(name)
	if field == nil || field.Kind() != protoreflect.MessageKind || !message.Has(field) {
		return ""
	}
	wrapper := message.Get(field).Message()
	valueField := wrapper.Descriptor().Fields().ByName("value")
	if valueField == nil || valueField.Kind() != protoreflect.StringKind {
		panic(fmt.Sprintf("durabletask: upstream protobuf field %q is not a StringValue", name))
	}
	return wrapper.Get(valueField).String()
}

func setString(message protoreflect.Message, name protoreflect.Name, value string) {
	field := message.Descriptor().Fields().ByName(name)
	if field == nil || field.Kind() != protoreflect.StringKind {
		panic(fmt.Sprintf("durabletask: upstream protobuf has no string field %q", name))
	}
	message.Set(field, protoreflect.ValueOfString(value))
}

func setStringWrapper(message protoreflect.Message, name protoreflect.Name, value string) {
	setStringWrapperIfPresent(message, name, &value)
}

func setStringWrapperIfPresent(message protoreflect.Message, name protoreflect.Name, value *string) {
	if value == nil {
		return
	}
	field := message.Descriptor().Fields().ByName(name)
	if field == nil || field.Kind() != protoreflect.MessageKind {
		panic(fmt.Sprintf("durabletask: upstream protobuf has no wrapper field %q", name))
	}
	message.Set(field, protoreflect.ValueOfMessage(wrapperspb.String(*value).ProtoReflect()))
}

func setStringList(message protoreflect.Message, name protoreflect.Name, values []string) {
	field := message.Descriptor().Fields().ByName(name)
	if field == nil || field.Kind() != protoreflect.StringKind || !field.IsList() {
		panic(fmt.Sprintf("durabletask: upstream protobuf has no string list field %q", name))
	}
	list := message.Mutable(field).List()
	for _, value := range values {
		list.Append(protoreflect.ValueOfString(value))
	}
}

func setInt32(message protoreflect.Message, name protoreflect.Name, value int32) {
	field := message.Descriptor().Fields().ByName(name)
	if field == nil || field.Kind() != protoreflect.Int32Kind {
		panic(fmt.Sprintf("durabletask: upstream protobuf has no int32 field %q", name))
	}
	message.Set(field, protoreflect.ValueOfInt32(value))
}

func setBool(message protoreflect.Message, name protoreflect.Name, value bool) {
	field := message.Descriptor().Fields().ByName(name)
	if field == nil || field.Kind() != protoreflect.BoolKind {
		panic(fmt.Sprintf("durabletask: upstream protobuf has no bool field %q", name))
	}
	message.Set(field, protoreflect.ValueOfBool(value))
}

func setTimestampIfPresent(message protoreflect.Message, name protoreflect.Name, value *time.Time) {
	if value == nil {
		return
	}
	field := message.Descriptor().Fields().ByName(name)
	if field == nil || field.Kind() != protoreflect.MessageKind {
		panic(fmt.Sprintf("durabletask: upstream protobuf has no timestamp field %q", name))
	}
	message.Set(field, protoreflect.ValueOfMessage(timestamppb.New(*value).ProtoReflect()))
}
