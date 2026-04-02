package bindings

import "encoding/json"

const EventGridTriggerBindingType BindingType = "eventGridTrigger"
// EventGridBinding is the JSON representation for EventGrid trigger bindings.
type EventGridBinding struct{}

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


