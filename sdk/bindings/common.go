package bindings

import (
	"encoding/json"
	"reflect"
)

type BindingType string

type BindingDirection int

const (
	In BindingDirection = iota
	Out
)

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
	*HTTPBinding
	*BlobBinding
	*EventGridBinding
	*TimerBinding
}

func (b Binding) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})
	m["name"] = b.Name
	m["type"] = b.Type
	m["direction"] = b.Direction

	var sub interface{}
	if b.CosmosDBBinding != nil {
		sub = b.CosmosDBBinding
	} else if b.HTTPBinding != nil {
		sub = b.HTTPBinding
	} else if b.BlobBinding != nil {
		sub = b.BlobBinding
	} else if b.EventGridBinding != nil {
		sub = b.EventGridBinding
	} else if b.TimerBinding != nil {
		sub = b.TimerBinding
	}

	if sub != nil {
		// Use a temporary map to merge fields
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

// SimpleBinding implements Bind for simple/generic bindings.
type SimpleBinding struct {
	Name      string
	Type      string
	Direction string
}

func (s SimpleBinding) GetBindingType() BindingType { return BindingType(s.Type) }
func (s SimpleBinding) ToBinding() Binding {
	return Binding{
		Name:      s.Name,
		Type:      s.Type,
		Direction: s.Direction,
	}
}

type Parameter struct {
	Name     string
	DataType reflect.Type
}
