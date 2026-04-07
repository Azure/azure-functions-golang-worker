package bindings

// HTTPTrigger is the binding type constant for HTTP triggers.
const HTTPTriggerType BindingType = "httpTrigger"

// HTTPBinding is the JSON representation for HTTP.
type HTTPBinding struct {
	AuthLevel string   `json:"authLevel"`
	Methods   []string `json:"methods"`
	Route     string   `json:"route,omitempty"`
}

// HTTPTrigger is the user-facing configuration for an HTTP trigger.
type HTTPTrigger struct {
	Name      string
	Route     string
	Methods   []string
	AuthLevel string // "anonymous", "function", "admin"
}

func (c *HTTPTrigger) GetBindingType() BindingType { return HTTPTriggerType }

func (c *HTTPTrigger) ToBinding() Binding {
	return Binding{
		Name:      c.Name,
		Type:      string(c.GetBindingType()),
		Direction: "in",
		HTTPBinding: &HTTPBinding{
			AuthLevel: c.AuthLevel,
			Methods:   c.Methods,
			Route:     c.Route,
		},
	}
}
