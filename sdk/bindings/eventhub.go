package bindings

// EventHubTriggerBindingType is the binding type constant for EventHub triggers.
const EventHubTriggerBindingType BindingType = "eventHubTrigger"

// EventHubBinding is the JSON representation for EventHub bindings.
type EventHubBinding struct {
	EventHubName  string `json:"eventHubName"`
	Connection    string `json:"connection"`
	ConsumerGroup string `json:"consumerGroup,omitempty"`
	Cardinality   string `json:"cardinality,omitempty"`
}

// EventHubMessage represents a message received from an Azure Event Hub.
type EventHubMessage struct {
	Body             string         `json:"body"`
	EnqueuedTimeUtc  string         `json:"enqueuedTimeUtc"`
	SequenceNumber   int64          `json:"sequenceNumber"`
	Offset           string         `json:"offset"`
	PartitionKey     string         `json:"partitionKey"`
	Properties       map[string]any `json:"properties"`
	SystemProperties map[string]any `json:"systemProperties"`
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
func (e *EventHubTrigger) GetBindingType() BindingType { return EventHubTriggerBindingType }

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
		},
	}
}


