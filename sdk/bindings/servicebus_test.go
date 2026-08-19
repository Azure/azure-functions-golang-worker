package bindings

import (
	"encoding/json"
	"testing"
)

func TestServiceBusQueueTrigger_ToBinding(t *testing.T) {
	trigger := &ServiceBusQueueTrigger{
		Name:        "message",
		QueueName:   "myqueue",
		Connection:  "ServiceBusConnection",
		Cardinality: "one",
	}

	binding := trigger.ToBinding()

	if binding.Type != "serviceBusTrigger" {
		t.Errorf("expected type %q, got %q", "serviceBusTrigger", binding.Type)
	}
	if binding.Direction != "in" {
		t.Errorf("expected direction %q, got %q", "in", binding.Direction)
	}
	if binding.ServiceBusBinding == nil {
		t.Fatal("expected ServiceBusBinding")
	}
	if binding.ServiceBusBinding.QueueName != "myqueue" {
		t.Errorf("expected queueName %q, got %q", "myqueue", binding.ServiceBusBinding.QueueName)
	}
	if binding.ServiceBusBinding.DataType != "binary" {
		t.Errorf("expected dataType %q, got %q", "binary", binding.ServiceBusBinding.DataType)
	}
}

func TestServiceBusTopicTrigger_ToBinding(t *testing.T) {
	trigger := &ServiceBusTopicTrigger{
		Name:             "message",
		TopicName:        "mytopic",
		SubscriptionName: "mysub",
		Connection:       "ServiceBusConnection",
		Cardinality:      "one",
	}

	binding := trigger.ToBinding()

	if binding.Type != "serviceBusTrigger" {
		t.Errorf("expected type %q, got %q", "serviceBusTrigger", binding.Type)
	}
	if binding.ServiceBusBinding.TopicName != "mytopic" {
		t.Errorf("expected topicName %q, got %q", "mytopic", binding.ServiceBusBinding.TopicName)
	}
	if binding.ServiceBusBinding.SubscriptionName != "mysub" {
		t.Errorf("expected subscriptionName %q, got %q", "mysub", binding.ServiceBusBinding.SubscriptionName)
	}
	if binding.ServiceBusBinding.DataType != "binary" {
		t.Errorf("expected dataType %q, got %q", "binary", binding.ServiceBusBinding.DataType)
	}
}

func TestServiceBusMessage_JSON(t *testing.T) {
	msg := ServiceBusMessage{
		Body:          json.RawMessage(`"hello world"`),
		MessageId:     "msg-123",
		DeliveryCount: 1,
		LockToken:     "token-abc",
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded ServiceBusMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if decoded.MessageId != "msg-123" {
		t.Errorf("expected messageId %q, got %q", "msg-123", decoded.MessageId)
	}
	if decoded.LockToken != "token-abc" {
		t.Errorf("expected lockToken %q, got %q", "token-abc", decoded.LockToken)
	}
	var encoded map[string]json.RawMessage
	if err := json.Unmarshal(data, &encoded); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := encoded["body"]; !ok {
		t.Error("expected body JSON property")
	}
	if _, ok := encoded["azfuncdata"]; ok {
		t.Error("did not expect legacy azfuncdata JSON property")
	}
}
