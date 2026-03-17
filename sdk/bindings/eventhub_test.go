package bindings

import (
	"encoding/json"
	"testing"
)

func TestEventHubTriggerGetBindingType(t *testing.T) {
	trigger := &EventHubTrigger{
		Name:          "message",
		EventHubName:  "myeventhub",
		Connection:    "EventHubConnection",
		ConsumerGroup: "$Default",
		Cardinality:   "one",
	}

	if got := trigger.GetBindingType(); got != EventHubTriggerBindingType {
		t.Errorf("GetBindingType() = %q, want %q", got, EventHubTriggerBindingType)
	}
}

func TestEventHubOutputGetBindingType(t *testing.T) {
	output := &EventHubOutput{
		Name:         "outputMessage",
		EventHubName: "myeventhub",
		Connection:   "EventHubConnection",
	}

	if got := output.GetBindingType(); got != EventHubOutputType {
		t.Errorf("GetBindingType() = %q, want %q", got, EventHubOutputType)
	}
}

func TestEventHubTriggerToBinding(t *testing.T) {
	trigger := &EventHubTrigger{
		Name:          "message",
		EventHubName:  "myeventhub",
		Connection:    "EventHubConnection",
		ConsumerGroup: "$Default",
		Cardinality:   "one",
	}

	b := trigger.ToBinding()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"Name", b.Name, "message"},
		{"Type", b.Type, "eventHubTrigger"},
		{"Direction", b.Direction, "in"},
	}
	for _, tc := range tests {
		if tc.got != tc.want {
			t.Errorf("Binding.%s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
	if b.EventHubBinding == nil {
		t.Fatal("Binding.EventHubBinding is nil")
	}
	if b.EventHubBinding.EventHubName != "myeventhub" {
		t.Errorf("EventHubBinding.EventHubName = %q, want %q", b.EventHubBinding.EventHubName, "myeventhub")
	}
	if b.EventHubBinding.Connection != "EventHubConnection" {
		t.Errorf("EventHubBinding.Connection = %q, want %q", b.EventHubBinding.Connection, "EventHubConnection")
	}
	if b.EventHubBinding.ConsumerGroup != "$Default" {
		t.Errorf("EventHubBinding.ConsumerGroup = %q, want %q", b.EventHubBinding.ConsumerGroup, "$Default")
	}
	if b.EventHubBinding.Cardinality != "one" {
		t.Errorf("EventHubBinding.Cardinality = %q, want %q", b.EventHubBinding.Cardinality, "one")
	}
}

func TestEventHubOutputToBinding(t *testing.T) {
	output := &EventHubOutput{
		Name:         "outputMessage",
		EventHubName: "myeventhub",
		Connection:   "EventHubConnection",
	}

	b := output.ToBinding()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"Name", b.Name, "outputMessage"},
		{"Type", b.Type, "eventHub"},
		{"Direction", b.Direction, "out"},
	}
	for _, tc := range tests {
		if tc.got != tc.want {
			t.Errorf("Binding.%s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
	if b.EventHubBinding == nil {
		t.Fatal("Binding.EventHubBinding is nil")
	}
	if b.EventHubBinding.EventHubName != "myeventhub" {
		t.Errorf("EventHubBinding.EventHubName = %q, want %q", b.EventHubBinding.EventHubName, "myeventhub")
	}
	if b.EventHubBinding.Connection != "EventHubConnection" {
		t.Errorf("EventHubBinding.Connection = %q, want %q", b.EventHubBinding.Connection, "EventHubConnection")
	}
}

func TestEventHubTriggerToBindingJSON(t *testing.T) {
	trigger := &EventHubTrigger{
		Name:          "message",
		EventHubName:  "myeventhub",
		Connection:    "EventHubConnection",
		ConsumerGroup: "$Default",
		Cardinality:   "one",
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
		"name":          "message",
		"type":          "eventHubTrigger",
		"direction":     "in",
		"eventHubName":  "myeventhub",
		"connection":    "EventHubConnection",
		"consumerGroup": "$Default",
		"cardinality":   "one",
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
}

func TestEventHubMessageDeserialization(t *testing.T) {
	input := `{
		"body": "hello world",
		"enqueuedTimeUtc": "2026-03-09T12:00:00Z",
		"sequenceNumber": 42,
		"offset": "1024",
		"partitionKey": "pk-1",
		"properties": {"key1": "val1"},
		"systemProperties": {"x-opt-enqueued-time": "2026-03-09T12:00:00Z"}
	}`

	var msg EventHubMessage
	if err := json.Unmarshal([]byte(input), &msg); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if msg.Body != "hello world" {
		t.Errorf("Body = %q, want %q", msg.Body, "hello world")
	}
	if msg.EnqueuedTimeUtc != "2026-03-09T12:00:00Z" {
		t.Errorf("EnqueuedTimeUtc = %q, want %q", msg.EnqueuedTimeUtc, "2026-03-09T12:00:00Z")
	}
	if msg.SequenceNumber != 42 {
		t.Errorf("SequenceNumber = %d, want %d", msg.SequenceNumber, 42)
	}
	if msg.Offset != "1024" {
		t.Errorf("Offset = %q, want %q", msg.Offset, "1024")
	}
	if msg.PartitionKey != "pk-1" {
		t.Errorf("PartitionKey = %q, want %q", msg.PartitionKey, "pk-1")
	}
	if msg.Properties["key1"] != "val1" {
		t.Errorf("Properties[key1] = %q, want %q", msg.Properties["key1"], "val1")
	}
}

func TestEventHubTriggerCardinalityVariations(t *testing.T) {
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
			trigger := &EventHubTrigger{
				Name:        "message",
				Cardinality: tc.cardinality,
			}
			b := trigger.ToBinding()
			if b.EventHubBinding.Cardinality != tc.want {
				t.Errorf("Cardinality = %q, want %q", b.EventHubBinding.Cardinality, tc.want)
			}
		})
	}
}

func TestEventHubTriggerConsumerGroupDefault(t *testing.T) {
	cases := []struct {
		name          string
		consumerGroup string
		want          string
	}{
		{"explicit consumer group", "myGroup", "myGroup"},
		{"default consumer group", "$Default", "$Default"},
		{"empty consumer group", "", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trigger := &EventHubTrigger{
				Name:          "message",
				ConsumerGroup: tc.consumerGroup,
			}
			b := trigger.ToBinding()
			if b.EventHubBinding.ConsumerGroup != tc.want {
				t.Errorf("ConsumerGroup = %q, want %q", b.EventHubBinding.ConsumerGroup, tc.want)
			}
		})
	}
}

func TestEventHubOutputJSON(t *testing.T) {
	output := &EventHubOutput{
		Name:         "outputMsg",
		EventHubName: "myeventhub",
		Connection:   "EventHubConnection",
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
		"name":         "outputMsg",
		"type":         "eventHub",
		"direction":    "out",
		"eventHubName": "myeventhub",
		"connection":   "EventHubConnection",
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

	// consumerGroup and cardinality should not appear for output
	for _, key := range []string{"consumerGroup", "cardinality"} {
		if _, ok := m[key]; ok {
			t.Errorf("JSON should not contain key %q for output binding", key)
		}
	}
}
