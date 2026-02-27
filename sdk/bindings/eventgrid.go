package bindings

import "encoding/json"

const EventGridTriggerBindingType BindingType = "eventGridTrigger"
const EventGridOutputType BindingType = "eventGrid"

// EventGridBinding is the JSON representation for EventGrid bindings.
type EventGridBinding struct {
	TopicEndpointUri string `json:"topicEndpointUri,omitempty"`
	TopicKeySetting  string `json:"topicKeySetting,omitempty"`
}

// EventGridEvent represents an Azure Event Grid event.
type EventGridEvent struct {
	Id              string          `json:"id"`
	Topic           string          `json:"topic"`
	Subject         string          `json:"subject"`
	EventType       string          `json:"eventType"`
	EventTime       string          `json:"eventTime"`
	DataVersion     string          `json:"dataVersion"`
	MetadataVersion string          `json:"metadataVersion"`
	Data            json.RawMessage `json:"data"`
}

// EventGridTrigger is the user-facing configuration for an EventGrid trigger.
type EventGridTrigger struct {
	Name string
}

func (e *EventGridTrigger) GetBindingType() BindingType { return EventGridTriggerBindingType }

func (e *EventGridTrigger) ToBinding() Binding {
	return Binding{
		Name:             e.Name,
		Type:             string(e.GetBindingType()),
		Direction:        "in",
		EventGridBinding: &EventGridBinding{},
	}
}

// EventGridOutput is the user-facing configuration for an EventGrid output binding.
type EventGridOutput struct {
	Name             string
	TopicEndpointUri string
	TopicKeySetting  string
}

func (e *EventGridOutput) GetBindingType() BindingType { return EventGridOutputType }

func (e *EventGridOutput) ToBinding() Binding {
	return Binding{
		Name:      e.Name,
		Type:      string(e.GetBindingType()),
		Direction: "out",
		EventGridBinding: &EventGridBinding{
			TopicEndpointUri: e.TopicEndpointUri,
			TopicKeySetting:  e.TopicKeySetting,
		},
	}
}
