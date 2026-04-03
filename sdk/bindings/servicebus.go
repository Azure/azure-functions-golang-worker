package bindings

import "encoding/json"

// ServiceBusTriggerBindingType is the binding type constant for Service Bus triggers.
const ServiceBusTriggerBindingType BindingType = "serviceBusTrigger"

// ServiceBusBinding is the JSON representation for Service Bus bindings.
type ServiceBusBinding struct {
	QueueName          string `json:"queueName,omitempty"`
	TopicName          string `json:"topicName,omitempty"`
	SubscriptionName   string `json:"subscriptionName,omitempty"`
	Connection         string `json:"connection"`
	IsSessionsEnabled  bool   `json:"isSessionsEnabled,omitempty"`
	Cardinality        string `json:"cardinality,omitempty"`
}

// ServiceBusMessage represents a message received from Azure Service Bus.
// The Body field uses the special json tag "azfuncdata" which instructs the
// worker's converter to populate this field from the raw trigger input data
// rather than from trigger metadata. Other fields are populated from trigger
// metadata using case-insensitive matching on their json tags.
type ServiceBusMessage struct {
	Body                    json.RawMessage        `json:"azfuncdata"`
	ContentType             string                 `json:"contentType"`
	CorrelationId           string                 `json:"correlationId"`
	DeliveryCount           int                    `json:"deliveryCount"`
	EnqueuedTimeUtc         string                 `json:"enqueuedTimeUtc"`
	ExpiresAtUtc            string                 `json:"expiresAtUtc"`
	Label                   string                 `json:"label"`
	LockToken               string                 `json:"lockToken"`
	MessageId               string                 `json:"messageId"`
	PartitionKey            string                 `json:"partitionKey"`
	ReplyTo                 string                 `json:"replyTo"`
	ReplyToSessionId        string                 `json:"replyToSessionId"`
	ScheduledEnqueueTimeUtc string                 `json:"scheduledEnqueueTimeUtc"`
	SequenceNumber          int64                  `json:"sequenceNumber"`
	SessionId               string                 `json:"sessionId"`
	State                   int                    `json:"state"`
	Subject                 string                 `json:"subject"`
	TimeToLive              string                 `json:"timeToLive"`
	To                      string                 `json:"to"`
	ApplicationProperties   map[string]interface{} `json:"applicationProperties"`
	UserProperties          map[string]interface{} `json:"userProperties"`
}


// ServiceBusQueueTrigger is the user-facing configuration for a Service Bus queue trigger.
type ServiceBusQueueTrigger struct {
	Name              string
	QueueName         string
	Connection        string
	IsSessionsEnabled bool
	Cardinality       string
}

// GetBindingType returns the Service Bus trigger binding type.
func (s *ServiceBusQueueTrigger) GetBindingType() BindingType { return ServiceBusTriggerBindingType }

// ToBinding converts the ServiceBusQueueTrigger to a Binding.
func (s *ServiceBusQueueTrigger) ToBinding() Binding {
	return Binding{
		Name:      s.Name,
		Type:      string(s.GetBindingType()),
		Direction: "in",
		ServiceBusBinding: &ServiceBusBinding{
			QueueName:         s.QueueName,
			Connection:        s.Connection,
			IsSessionsEnabled: s.IsSessionsEnabled,
			Cardinality:       s.Cardinality,
		},
	}
}

// ServiceBusTopicTrigger is the user-facing configuration for a Service Bus topic trigger.
type ServiceBusTopicTrigger struct {
	Name              string
	TopicName         string
	SubscriptionName  string
	Connection        string
	IsSessionsEnabled bool
	Cardinality       string
}

// GetBindingType returns the Service Bus trigger binding type.
func (s *ServiceBusTopicTrigger) GetBindingType() BindingType { return ServiceBusTriggerBindingType }

// ToBinding converts the ServiceBusTopicTrigger to a Binding.
func (s *ServiceBusTopicTrigger) ToBinding() Binding {
	return Binding{
		Name:      s.Name,
		Type:      string(s.GetBindingType()),
		Direction: "in",
		ServiceBusBinding: &ServiceBusBinding{
			TopicName:         s.TopicName,
			SubscriptionName:  s.SubscriptionName,
			Connection:        s.Connection,
			IsSessionsEnabled: s.IsSessionsEnabled,
			Cardinality:       s.Cardinality,
		},
	}
}


