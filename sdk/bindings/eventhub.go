package bindings

import "encoding/json"

// EventHubTrigger is the binding type constant for EventHub triggers.
const EventHubTriggerType BindingType = "eventHubTrigger"

// EventHubBinding is the JSON representation for EventHub bindings.
type EventHubBinding struct {
	EventHubName  string `json:"eventHubName"`
	Connection    string `json:"connection"`
	ConsumerGroup string `json:"consumerGroup,omitempty"`
	Cardinality   string `json:"cardinality,omitempty"`
	DataType      string `json:"dataType"`
}

// EventHubMessage represents a message received from an Azure Event Hub.
// The Body field is populated from the "body" key in trigger metadata.
// Other fields (SequenceNumber, Offset, etc.) are also populated from
// trigger metadata using case-insensitive matching on their json tags.
type EventHubMessage struct {
	Body             json.RawMessage `json:"body" azfunc:"data"`
	EnqueuedTimeUtc  string          `json:"enqueuedTimeUtc"`
	SequenceNumber   int64           `json:"sequenceNumber"`
	Offset           string          `json:"offset"`
	PartitionKey     string          `json:"partitionKey"`
	Properties       map[string]any  `json:"properties"`
	SystemProperties map[string]any  `json:"systemProperties"`
}

// EventHubTrigger is the user-facing configuration for an EventHub trigger.
type EventHubTrigger struct {
	Name          string
	EventHubName  string
	Connection    string
	ConsumerGroup string
	Cardinality   string
}

// GetBindingType returns the EventHub trigger binding type.
func (e *EventHubTrigger) GetBindingType() BindingType { return EventHubTriggerType }

// ToBinding converts the EventHubTrigger to a Binding.
func (e *EventHubTrigger) ToBinding() Binding {
	return Binding{
		Name:      e.Name,
		Type:      string(e.GetBindingType()),
		Direction: "in",
		EventHubBinding: &EventHubBinding{
			EventHubName:  e.EventHubName,
			Connection:    e.Connection,
			ConsumerGroup: e.ConsumerGroup,
			Cardinality:   e.Cardinality,
			DataType:      "binary", // Preserve event bodies as bytes for both single events and batches.
		},
	}
}
