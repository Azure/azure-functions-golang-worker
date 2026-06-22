package bindings

import (
	"encoding/json"
	"testing"
)

func TestQueueStorageTrigger_ToBinding(t *testing.T) {
	trigger := &QueueStorageTrigger{
		Name:       "message",
		QueueName:  "myqueue",
		Connection: "AzureWebJobsStorage",
	}

	binding := trigger.ToBinding()

	if binding.Type != "queueTrigger" {
		t.Errorf("expected type %q, got %q", "queueTrigger", binding.Type)
	}
	if binding.Direction != "in" {
		t.Errorf("expected direction %q, got %q", "in", binding.Direction)
	}
	if binding.QueueBinding == nil {
		t.Fatal("expected QueueBinding")
	}
	if binding.QueueBinding.QueueName != "myqueue" {
		t.Errorf("expected queueName %q, got %q", "myqueue", binding.QueueBinding.QueueName)
	}
	if binding.QueueBinding.Connection != "AzureWebJobsStorage" {
		t.Errorf("expected connection %q, got %q", "AzureWebJobsStorage", binding.QueueBinding.Connection)
	}
}

func TestQueueStorageTrigger_GetBindingType(t *testing.T) {
	trigger := &QueueStorageTrigger{Name: "msg"}
	if trigger.GetBindingType() != QueueTriggerType {
		t.Errorf("expected %q, got %q", QueueTriggerType, trigger.GetBindingType())
	}
}

func TestQueueStorageTrigger_MarshalJSON(t *testing.T) {
	trigger := &QueueStorageTrigger{
		Name:       "message",
		QueueName:  "testqueue",
		Connection: "StorageConn",
	}

	binding := trigger.ToBinding()
	data, err := json.Marshal(binding)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}

	if m["type"] != "queueTrigger" {
		t.Errorf("expected type %q, got %v", "queueTrigger", m["type"])
	}
	if m["direction"] != "in" {
		t.Errorf("expected direction %q, got %v", "in", m["direction"])
	}
	if m["queueName"] != "testqueue" {
		t.Errorf("expected queueName %q, got %v", "testqueue", m["queueName"])
	}
	if m["connection"] != "StorageConn" {
		t.Errorf("expected connection %q, got %v", "StorageConn", m["connection"])
	}
}

func TestQueueMessage_JSON(t *testing.T) {
	msg := QueueMessage{
		Body:           json.RawMessage(`"hello world"`),
		Id:             "msg-456",
		PopReceipt:     "receipt-abc",
		DequeueCount:   3,
		ExpirationTime: "2026-07-01T00:00:00Z",
		InsertionTime:  "2026-06-22T10:00:00Z",
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded QueueMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if decoded.Id != "msg-456" {
		t.Errorf("expected id %q, got %q", "msg-456", decoded.Id)
	}
	if decoded.PopReceipt != "receipt-abc" {
		t.Errorf("expected popReceipt %q, got %q", "receipt-abc", decoded.PopReceipt)
	}
	if decoded.DequeueCount != 3 {
		t.Errorf("expected dequeueCount %d, got %d", 3, decoded.DequeueCount)
	}
}
