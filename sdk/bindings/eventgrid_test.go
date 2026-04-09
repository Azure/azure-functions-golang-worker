package bindings

import (
	"encoding/json"
	"testing"
)

func TestEventGridTrigger_ToBinding(t *testing.T) {
	trigger := &EventGridTrigger{
		Name: "event",
	}

	binding := trigger.ToBinding()

	if binding.Type != "eventGridTrigger" {
		t.Errorf("expected type %q, got %q", "eventGridTrigger", binding.Type)
	}
	if binding.Direction != "in" {
		t.Errorf("expected direction %q, got %q", "in", binding.Direction)
	}
	if binding.Name != "event" {
		t.Errorf("expected name %q, got %q", "event", binding.Name)
	}
}

func TestEventGridTriggerType(t *testing.T) {
	trigger := &EventGridTrigger{}
	if trigger.GetBindingType() != EventGridTriggerType {
		t.Errorf("expected %q, got %q", EventGridTriggerType, trigger.GetBindingType())
	}
}

func TestEventGridEvent_JSON(t *testing.T) {
	event := EventGridEvent{
		Id:        "evt-123",
		EventType: "Microsoft.Storage.BlobCreated",
		Subject:   "/blobServices/default/containers/test",
		Data:      json.RawMessage(`{"api":"PutBlob"}`),
	}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded EventGridEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if decoded.Id != "evt-123" {
		t.Errorf("expected id %q, got %q", "evt-123", decoded.Id)
	}
	if decoded.EventType != "Microsoft.Storage.BlobCreated" {
		t.Errorf("expected eventType %q, got %q", "Microsoft.Storage.BlobCreated", decoded.EventType)
	}
}

func TestEventGridBinding_MarshalJSON(t *testing.T) {
	// EventGrid trigger has no sub-binding properties — should serialize
	// with just name, type, direction
	binding := Binding{
		Name:      "event",
		Type:      "eventGridTrigger",
		Direction: "in",
	}

	data, err := json.Marshal(binding)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != 3 {
		t.Errorf("expected 3 fields (name, type, direction), got %d: %v", len(result), result)
	}
}
