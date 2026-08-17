package bindings

import "encoding/json"

// QueueTriggerType is the binding type constant for Azure Storage Queue triggers.
const QueueTriggerType BindingType = "queueTrigger"

// QueueBinding is the JSON representation for Storage Queue bindings.
type QueueBinding struct {
	QueueName  string `json:"queueName,omitempty"`
	Connection string `json:"connection,omitempty"`
}

// QueueStorageTrigger is the user-facing configuration for a Storage Queue trigger.
type QueueStorageTrigger struct {
	Name       string
	QueueName  string
	Connection string
}

// GetBindingType returns the Storage Queue trigger binding type.
func (q *QueueStorageTrigger) GetBindingType() BindingType { return QueueTriggerType }

// ToBinding converts the QueueStorageTrigger to a Binding.
func (q *QueueStorageTrigger) ToBinding() Binding {
	return Binding{
		Name:      q.Name,
		Type:      string(q.GetBindingType()),
		Direction: "in",
		QueueBinding: &QueueBinding{
			QueueName:  q.QueueName,
			Connection: q.Connection,
		},
	}
}

// QueueMessage represents a message received from Azure Storage Queue.
// The Body field is populated from the raw trigger input.
// Other fields are populated from trigger metadata using case-insensitive
// matching on their json tags.
type QueueMessage struct {
	Body            json.RawMessage `json:"body" azfunc:"data"`
	Id              string          `json:"id"`
	PopReceipt      string          `json:"popReceipt"`
	DequeueCount    int64           `json:"dequeueCount"`
	ExpirationTime  string          `json:"expirationTime"`
	InsertionTime   string          `json:"insertionTime"`
	NextVisibleTime string          `json:"nextVisibleTime"`
}
