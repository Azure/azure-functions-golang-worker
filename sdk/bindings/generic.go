package bindings

import (
	"encoding/json"
	"fmt"
	"strings"
)

// GenericBinding is a host binding's configuration without a trigger-specific
// Go model. Properties use the host's exact JSON names and are flattened into
// the binding object. Values must be JSON-serializable. Setting references and
// binding expressions are passed through for the host to resolve.
type GenericBinding struct {
	DataType    string
	Cardinality string
	Properties  map[string]any
}

// GenericTrigger describes a host-provided, non-HTTP trigger. Its host extension
// must already be installed and support the native worker and hosting plan.
// DataType may be empty, "string", or "binary". Cardinality may be empty,
// "one", or "many"; a slice parameter alone does not request host batching.
type GenericTrigger struct {
	Type        string
	Name        string
	DataType    string
	Cardinality string
	Properties  map[string]any
}

// GetBindingType returns the host extension's binding type.
func (g *GenericTrigger) GetBindingType() BindingType { return BindingType(g.Type) }

// ToBinding implements Bind. Registration validates and snapshots the metadata.
func (g *GenericTrigger) ToBinding() Binding {
	if g == nil {
		panic("GenericTrigger: trigger must not be nil")
	}
	return Binding{
		Name: g.Name, Type: g.Type, Direction: "in",
		GenericBinding: &GenericBinding{
			DataType: g.DataType, Cardinality: g.Cardinality, Properties: g.Properties,
		},
	}
}

func (b Binding) marshalGeneric() ([]byte, error) {
	if b.CosmosDBBinding != nil || b.HTTPBinding != nil || b.BlobBinding != nil ||
		b.TimerBinding != nil || b.EventHubBinding != nil || b.ServiceBusBinding != nil ||
		b.SQLBinding != nil || b.QueueBinding != nil {
		return nil, fmt.Errorf("binding %q: generic and typed configuration cannot be combined", b.Name)
	}
	g := b.GenericBinding
	switch g.DataType {
	case "", "string", "binary":
	default:
		return nil, fmt.Errorf("binding %q: dataType must be string or binary, or omitted", b.Name)
	}
	switch g.Cardinality {
	case "", "one", "many":
	default:
		return nil, fmt.Errorf("binding %q: cardinality must be one or many, or omitted", b.Name)
	}
	m := map[string]any{"name": b.Name, "type": b.Type, "direction": b.Direction}
	if g.DataType != "" {
		m["dataType"] = g.DataType
	}
	if g.Cardinality != "" {
		m["cardinality"] = g.Cardinality
	}
	seen := make(map[string]bool, len(g.Properties))
	for key, value := range g.Properties {
		canonical := strings.ToLower(key)
		switch canonical {
		case "name", "type", "direction", "datatype", "cardinality":
			return nil, fmt.Errorf("binding %q: property %q is reserved; use the descriptor field", b.Name, key)
		}
		if strings.TrimSpace(key) == "" || seen[canonical] {
			return nil, fmt.Errorf("binding %q: empty or duplicate property %q", b.Name, key)
		}
		seen[canonical] = true
		m[key] = value
	}
	return json.Marshal(m)
}
