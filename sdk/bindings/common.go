package bindings

import (
	"encoding/json"
)

// BindingType identifies the type of a trigger or binding.
type BindingType string

// Bind is the interface that all bindings must implement.
type Bind interface {
	GetBindingType() BindingType
	ToBinding() Binding
}

// Binding represents the internal JSON structure of a binding.
type Binding struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Direction string `json:"direction"`

	*CosmosDBBinding
	*HttpBinding
	*BlobBinding
	*EventGridBinding
	*TimerBinding
	*EventHubBinding
	*ServiceBusBinding
}

func (b Binding) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})
	m["name"] = b.Name
	m["type"] = b.Type
	m["direction"] = b.Direction

	var sub interface{}
	if b.CosmosDBBinding != nil {
		sub = b.CosmosDBBinding
	} else if b.HttpBinding != nil {
		sub = b.HttpBinding
	} else if b.BlobBinding != nil {
		sub = b.BlobBinding
	} else if b.EventGridBinding != nil {
		sub = b.EventGridBinding
	} else if b.TimerBinding != nil {
		sub = b.TimerBinding
	} else if b.EventHubBinding != nil {
		sub = b.EventHubBinding
	} else if b.ServiceBusBinding != nil {
		sub = b.ServiceBusBinding
	}

	if sub != nil {
		data, err := json.Marshal(sub)
		if err != nil {
			return nil, err
		}
		var temp map[string]interface{}
		if err := json.Unmarshal(data, &temp); err != nil {
			return nil, err
		}
		for k, v := range temp {
			m[k] = v
		}
	}

	return json.Marshal(m)
}
