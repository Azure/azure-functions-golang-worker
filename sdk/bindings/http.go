package bindings

// HttpTriggerBindingType is the binding type constant for HTTP triggers.
const HttpTriggerBindingType BindingType = "httpTrigger"

// HttpBinding is the JSON representation for HTTP.
type HttpBinding struct {
	AuthLevel string   `json:"authLevel"`
	Methods   []string `json:"methods"`
	Route     string   `json:"route,omitempty"`
}

// HttpTrigger is the user-facing configuration for an HTTP trigger.
type HttpTrigger struct {
	Name      string
	Route     string
	Methods   []string
	AuthLevel string // "anonymous", "function", "admin"
}

func (c *HttpTrigger) GetBindingType() BindingType { return HttpTriggerBindingType }

func (c *HttpTrigger) ToBinding() Binding {
	return Binding{
		Name:      c.Name,
		Type:      string(c.GetBindingType()),
		Direction: "in",
		HttpBinding: &HttpBinding{
			AuthLevel: c.AuthLevel,
			Methods:   c.Methods,
			Route:     c.Route,
		},
	}
}
