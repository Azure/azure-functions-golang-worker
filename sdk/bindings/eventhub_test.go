package bindings

import (
	"encoding/json"
	"testing"
)

func TestEventHubTrigger_ToBinding(t *testing.T) {
	trigger := &EventHubTrigger{
		Name:          "message",
		EventHubName:  "myeventhub",
		Connection:    "EventHubConnection",
		ConsumerGroup: "$Default",
		Cardinality:   "one",
	}

	binding := trigger.ToBinding()

	if binding.Type != "eventHubTrigger" {
		t.Errorf("expected type %q, got %q", "eventHubTrigger", binding.Type)
	}
	if binding.Direction != "in" {
		t.Errorf("expected direction %q, got %q", "in", binding.Direction)
	}
	if binding.EventHubBinding == nil {
		t.Fatal("expected EventHubBinding")
	}
	if binding.EventHubBinding.EventHubName != "myeventhub" {
		t.Errorf("expected eventHubName %q, got %q", "myeventhub", binding.EventHubBinding.EventHubName)
	}
}

func TestEventHubMessage_JSON(t *testing.T) {
	msg := EventHubMessage{
		Body:            json.RawMessage(`"hello"`),
		SequenceNumber:  42,
		Offset:          "1024",
		EnqueuedTimeUtc: "2026-01-01T00:00:00Z",
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded EventHubMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if decoded.SequenceNumber != 42 {
		t.Errorf("expected sequence number 42, got %d", decoded.SequenceNumber)
	}
}
