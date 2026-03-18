package bindings

import (
	"encoding/json"
	"testing"
)

func TestServiceBusQueueTriggerGetBindingType(t *testing.T) {
	trigger := &ServiceBusQueueTrigger{
		Name:       "message",
		QueueName:  "myqueue",
		Connection: "ServiceBusConnection",
	}

	if got := trigger.GetBindingType(); got != ServiceBusTriggerBindingType {
		t.Errorf("GetBindingType() = %q, want %q", got, ServiceBusTriggerBindingType)
	}
}

func TestServiceBusTopicTriggerGetBindingType(t *testing.T) {
	trigger := &ServiceBusTopicTrigger{
		Name:             "message",
		TopicName:        "mytopic",
		SubscriptionName: "mysub",
		Connection:       "ServiceBusConnection",
	}

	if got := trigger.GetBindingType(); got != ServiceBusTriggerBindingType {
		t.Errorf("GetBindingType() = %q, want %q", got, ServiceBusTriggerBindingType)
	}
}

func TestServiceBusQueueOutputGetBindingType(t *testing.T) {
	output := &ServiceBusQueueOutput{
		Name:       "outputMessage",
		QueueName:  "myqueue",
		Connection: "ServiceBusConnection",
	}

	if got := output.GetBindingType(); got != ServiceBusOutputType {
		t.Errorf("GetBindingType() = %q, want %q", got, ServiceBusOutputType)
	}
}

func TestServiceBusTopicOutputGetBindingType(t *testing.T) {
	output := &ServiceBusTopicOutput{
		Name:       "outputMessage",
		TopicName:  "mytopic",
		Connection: "ServiceBusConnection",
	}

	if got := output.GetBindingType(); got != ServiceBusOutputType {
		t.Errorf("GetBindingType() = %q, want %q", got, ServiceBusOutputType)
	}
}

func TestServiceBusQueueTriggerToBinding(t *testing.T) {
	trigger := &ServiceBusQueueTrigger{
		Name:        "message",
		QueueName:   "myqueue",
		Connection:  "ServiceBusConnection",
		Cardinality: "one",
	}

	b := trigger.ToBinding()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"Name", b.Name, "message"},
		{"Type", b.Type, "serviceBusTrigger"},
		{"Direction", b.Direction, "in"},
	}
	for _, tc := range tests {
		if tc.got != tc.want {
			t.Errorf("Binding.%s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
	if b.ServiceBusBinding == nil {
		t.Fatal("Binding.ServiceBusBinding is nil")
	}
	if b.ServiceBusBinding.QueueName != "myqueue" {
		t.Errorf("ServiceBusBinding.QueueName = %q, want %q", b.ServiceBusBinding.QueueName, "myqueue")
	}
	if b.ServiceBusBinding.Connection != "ServiceBusConnection" {
		t.Errorf("ServiceBusBinding.Connection = %q, want %q", b.ServiceBusBinding.Connection, "ServiceBusConnection")
	}
	if b.ServiceBusBinding.Cardinality != "one" {
		t.Errorf("ServiceBusBinding.Cardinality = %q, want %q", b.ServiceBusBinding.Cardinality, "one")
	}
}

func TestServiceBusTopicTriggerToBinding(t *testing.T) {
	trigger := &ServiceBusTopicTrigger{
		Name:             "message",
		TopicName:        "mytopic",
		SubscriptionName: "mysub",
		Connection:       "ServiceBusConnection",
		Cardinality:      "one",
	}

	b := trigger.ToBinding()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"Name", b.Name, "message"},
		{"Type", b.Type, "serviceBusTrigger"},
		{"Direction", b.Direction, "in"},
	}
	for _, tc := range tests {
		if tc.got != tc.want {
			t.Errorf("Binding.%s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
	if b.ServiceBusBinding == nil {
		t.Fatal("Binding.ServiceBusBinding is nil")
	}
	if b.ServiceBusBinding.TopicName != "mytopic" {
		t.Errorf("ServiceBusBinding.TopicName = %q, want %q", b.ServiceBusBinding.TopicName, "mytopic")
	}
	if b.ServiceBusBinding.SubscriptionName != "mysub" {
		t.Errorf("ServiceBusBinding.SubscriptionName = %q, want %q", b.ServiceBusBinding.SubscriptionName, "mysub")
	}
	if b.ServiceBusBinding.Connection != "ServiceBusConnection" {
		t.Errorf("ServiceBusBinding.Connection = %q, want %q", b.ServiceBusBinding.Connection, "ServiceBusConnection")
	}
}

func TestServiceBusQueueOutputToBinding(t *testing.T) {
	output := &ServiceBusQueueOutput{
		Name:       "outputMessage",
		QueueName:  "myqueue",
		Connection: "ServiceBusConnection",
	}

	b := output.ToBinding()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"Name", b.Name, "outputMessage"},
		{"Type", b.Type, "serviceBus"},
		{"Direction", b.Direction, "out"},
	}
	for _, tc := range tests {
		if tc.got != tc.want {
			t.Errorf("Binding.%s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
	if b.ServiceBusBinding == nil {
		t.Fatal("Binding.ServiceBusBinding is nil")
	}
	if b.ServiceBusBinding.QueueName != "myqueue" {
		t.Errorf("ServiceBusBinding.QueueName = %q, want %q", b.ServiceBusBinding.QueueName, "myqueue")
	}
}

func TestServiceBusTopicOutputToBinding(t *testing.T) {
	output := &ServiceBusTopicOutput{
		Name:       "outputMessage",
		TopicName:  "mytopic",
		Connection: "ServiceBusConnection",
	}

	b := output.ToBinding()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"Name", b.Name, "outputMessage"},
		{"Type", b.Type, "serviceBus"},
		{"Direction", b.Direction, "out"},
	}
	for _, tc := range tests {
		if tc.got != tc.want {
			t.Errorf("Binding.%s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
	if b.ServiceBusBinding == nil {
		t.Fatal("Binding.ServiceBusBinding is nil")
	}
	if b.ServiceBusBinding.TopicName != "mytopic" {
		t.Errorf("ServiceBusBinding.TopicName = %q, want %q", b.ServiceBusBinding.TopicName, "mytopic")
	}
}

func TestServiceBusQueueTriggerToBindingJSON(t *testing.T) {
	trigger := &ServiceBusQueueTrigger{
		Name:        "message",
		QueueName:   "myqueue",
		Connection:  "ServiceBusConnection",
		Cardinality: "one",
	}

	data, err := json.Marshal(trigger.ToBinding())
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	want := map[string]string{
		"name":        "message",
		"type":        "serviceBusTrigger",
		"direction":   "in",
		"queueName":   "myqueue",
		"connection":  "ServiceBusConnection",
		"cardinality": "one",
	}
	for key, wantVal := range want {
		got, ok := m[key]
		if !ok {
			t.Errorf("JSON missing key %q", key)
			continue
		}
		if got != wantVal {
			t.Errorf("JSON[%q] = %v, want %q", key, got, wantVal)
		}
	}

	// topicName and subscriptionName should not appear for queue trigger
	for _, key := range []string{"topicName", "subscriptionName"} {
		if _, ok := m[key]; ok {
			t.Errorf("JSON should not contain key %q for queue trigger", key)
		}
	}
}

func TestServiceBusTopicTriggerToBindingJSON(t *testing.T) {
	trigger := &ServiceBusTopicTrigger{
		Name:             "message",
		TopicName:        "mytopic",
		SubscriptionName: "mysub",
		Connection:       "ServiceBusConnection",
		Cardinality:      "one",
	}

	data, err := json.Marshal(trigger.ToBinding())
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	want := map[string]string{
		"name":             "message",
		"type":             "serviceBusTrigger",
		"direction":        "in",
		"topicName":        "mytopic",
		"subscriptionName": "mysub",
		"connection":       "ServiceBusConnection",
		"cardinality":      "one",
	}
	for key, wantVal := range want {
		got, ok := m[key]
		if !ok {
			t.Errorf("JSON missing key %q", key)
			continue
		}
		if got != wantVal {
			t.Errorf("JSON[%q] = %v, want %q", key, got, wantVal)
		}
	}

	// queueName should not appear for topic trigger
	if _, ok := m["queueName"]; ok {
		t.Error("JSON should not contain key \"queueName\" for topic trigger")
	}
}

func TestServiceBusQueueOutputJSON(t *testing.T) {
	output := &ServiceBusQueueOutput{
		Name:       "outputMsg",
		QueueName:  "myqueue",
		Connection: "ServiceBusConnection",
	}

	data, err := json.Marshal(output.ToBinding())
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	want := map[string]string{
		"name":       "outputMsg",
		"type":       "serviceBus",
		"direction":  "out",
		"queueName":  "myqueue",
		"connection": "ServiceBusConnection",
	}
	for key, wantVal := range want {
		got, ok := m[key]
		if !ok {
			t.Errorf("JSON missing key %q", key)
			continue
		}
		if got != wantVal {
			t.Errorf("JSON[%q] = %v, want %q", key, got, wantVal)
		}
	}

	// cardinality and isSessionsEnabled should not appear for output
	for _, key := range []string{"cardinality", "isSessionsEnabled"} {
		if _, ok := m[key]; ok {
			t.Errorf("JSON should not contain key %q for output binding", key)
		}
	}
}

func TestServiceBusTopicOutputJSON(t *testing.T) {
	output := &ServiceBusTopicOutput{
		Name:       "outputMsg",
		TopicName:  "mytopic",
		Connection: "ServiceBusConnection",
	}

	data, err := json.Marshal(output.ToBinding())
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	want := map[string]string{
		"name":       "outputMsg",
		"type":       "serviceBus",
		"direction":  "out",
		"topicName":  "mytopic",
		"connection": "ServiceBusConnection",
	}
	for key, wantVal := range want {
		got, ok := m[key]
		if !ok {
			t.Errorf("JSON missing key %q", key)
			continue
		}
		if got != wantVal {
			t.Errorf("JSON[%q] = %v, want %q", key, got, wantVal)
		}
	}

	// cardinality, isSessionsEnabled, queueName should not appear for topic output
	for _, key := range []string{"cardinality", "isSessionsEnabled", "queueName"} {
		if _, ok := m[key]; ok {
			t.Errorf("JSON should not contain key %q for topic output binding", key)
		}
	}
}

func TestServiceBusMessageDeserialization(t *testing.T) {
	// Test that metadata fields deserialize correctly.
	// Note: Body uses json:"azfuncdata" tag for InputData mapping,
	// so it won't be populated from a standard JSON object.
	// This test focuses on the metadata fields.
	input := `{
		"contentType": "application/json",
		"correlationId": "corr-123",
		"deliveryCount": 1,
		"enqueuedTimeUtc": "2026-03-17T12:00:00Z",
		"expiresAtUtc": "2026-03-18T12:00:00Z",
		"label": "test-label",
		"lockToken": "lock-abc",
		"messageId": "msg-456",
		"partitionKey": "pk-1",
		"replyTo": "reply-queue",
		"replyToSessionId": "session-reply",
		"scheduledEnqueueTimeUtc": "2026-03-17T13:00:00Z",
		"sequenceNumber": 42,
		"sessionId": "session-1",
		"state": 0,
		"subject": "test-subject",
		"timeToLive": "01:00:00",
		"to": "destination",
		"applicationProperties": {"key1": "val1"},
		"userProperties": {"key2": "val2"}
	}`

	var msg ServiceBusMessage
	if err := json.Unmarshal([]byte(input), &msg); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if msg.ContentType != "application/json" {
		t.Errorf("ContentType = %q, want %q", msg.ContentType, "application/json")
	}
	if msg.CorrelationId != "corr-123" {
		t.Errorf("CorrelationId = %q, want %q", msg.CorrelationId, "corr-123")
	}
	if msg.DeliveryCount != 1 {
		t.Errorf("DeliveryCount = %d, want %d", msg.DeliveryCount, 1)
	}
	if msg.EnqueuedTimeUtc != "2026-03-17T12:00:00Z" {
		t.Errorf("EnqueuedTimeUtc = %q, want %q", msg.EnqueuedTimeUtc, "2026-03-17T12:00:00Z")
	}
	if msg.ExpiresAtUtc != "2026-03-18T12:00:00Z" {
		t.Errorf("ExpiresAtUtc = %q, want %q", msg.ExpiresAtUtc, "2026-03-18T12:00:00Z")
	}
	if msg.MessageId != "msg-456" {
		t.Errorf("MessageId = %q, want %q", msg.MessageId, "msg-456")
	}
	if msg.SequenceNumber != 42 {
		t.Errorf("SequenceNumber = %d, want %d", msg.SequenceNumber, 42)
	}
	if msg.SessionId != "session-1" {
		t.Errorf("SessionId = %q, want %q", msg.SessionId, "session-1")
	}
	if msg.ApplicationProperties["key1"] != "val1" {
		t.Errorf("ApplicationProperties[key1] = %q, want %q", msg.ApplicationProperties["key1"], "val1")
	}
	if msg.UserProperties["key2"] != "val2" {
		t.Errorf("UserProperties[key2] = %q, want %q", msg.UserProperties["key2"], "val2")
	}
}

func TestServiceBusTriggerCardinalityVariations(t *testing.T) {
	cases := []struct {
		name        string
		cardinality string
		want        string
	}{
		{"cardinality one", "one", "one"},
		{"cardinality many", "many", "many"},
		{"cardinality empty", "", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trigger := &ServiceBusQueueTrigger{
				Name:        "message",
				Cardinality: tc.cardinality,
			}
			b := trigger.ToBinding()
			if b.ServiceBusBinding.Cardinality != tc.want {
				t.Errorf("Cardinality = %q, want %q", b.ServiceBusBinding.Cardinality, tc.want)
			}
		})
	}
}

func TestServiceBusTriggerSessionsEnabled(t *testing.T) {
	cases := []struct {
		name    string
		enabled bool
		want    bool
	}{
		{"sessions enabled", true, true},
		{"sessions disabled", false, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trigger := &ServiceBusQueueTrigger{
				Name:              "message",
				IsSessionsEnabled: tc.enabled,
			}
			b := trigger.ToBinding()
			if b.ServiceBusBinding.IsSessionsEnabled != tc.want {
				t.Errorf("IsSessionsEnabled = %v, want %v", b.ServiceBusBinding.IsSessionsEnabled, tc.want)
			}
		})
	}
}

func TestDeserializeServiceBusMessage(t *testing.T) {
	input := `{"messageId":"msg-789","deliveryCount":3,"sequenceNumber":100}`

	msg := DeserializeServiceBusMessage(input)

	if msg.MessageId != "msg-789" {
		t.Errorf("MessageId = %q, want %q", msg.MessageId, "msg-789")
	}
	if msg.DeliveryCount != 3 {
		t.Errorf("DeliveryCount = %d, want %d", msg.DeliveryCount, 3)
	}
	if msg.SequenceNumber != 100 {
		t.Errorf("SequenceNumber = %d, want %d", msg.SequenceNumber, 100)
	}
	// Body won't be populated from JSON since it uses azfuncdata tag
	if msg.Body != nil {
		t.Errorf("Body should be nil when not present in JSON with azfuncdata tag, got %s", string(msg.Body))
	}
}
