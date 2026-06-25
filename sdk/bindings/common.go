package bindings

import (
	"encoding/json"
	"reflect"
	"strings"
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
	*HTTPBinding
	*BlobBinding
	*TimerBinding
	*EventHubBinding
	*ServiceBusBinding
	*SQLBinding
	*QueueBinding
}

// MarshalJSON flattens the embedded sub-binding fields into the top-level
// JSON object alongside name, type, and direction.
func (b Binding) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})
	m["name"] = b.Name
	m["type"] = b.Type
	m["direction"] = b.Direction

	// Merge sub-binding fields directly via reflection instead of
	// marshal → unmarshal → merge round-trip.
	var sub interface{}
	if b.CosmosDBBinding != nil {
		sub = b.CosmosDBBinding
	} else if b.HTTPBinding != nil {
		sub = b.HTTPBinding
	} else if b.BlobBinding != nil {
		sub = b.BlobBinding
	} else if b.TimerBinding != nil {
		sub = b.TimerBinding
	} else if b.EventHubBinding != nil {
		sub = b.EventHubBinding
	} else if b.ServiceBusBinding != nil {
		sub = b.ServiceBusBinding
	} else if b.SQLBinding != nil {
		sub = b.SQLBinding
	} else if b.QueueBinding != nil {
		sub = b.QueueBinding
	}

	if sub != nil {
		v := reflect.ValueOf(sub)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		t := v.Type()
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			tag := field.Tag.Get("json")
			if tag == "" || tag == "-" {
				continue
			}
			// Handle "name,omitempty" style tags
			name, opts, _ := strings.Cut(tag, ",")
			val := v.Field(i)
			if strings.Contains(opts, "omitempty") && val.IsZero() {
				continue
			}
			m[name] = val.Interface()
		}
	}

	return json.Marshal(m)
}
