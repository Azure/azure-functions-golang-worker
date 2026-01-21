package sdk

import (
	"encoding/json"
	"reflect"
)

type BindingType string

const (
	CosmosDBBindingType BindingType = "cosmosDBTrigger"
	HttpBindingType     BindingType = "httpTrigger"
)

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
}

// Parameter represents a function parameter.
type Parameter struct {
	Name     string
	DataType reflect.Type
}

// CosmosDBBinding is the JSON representation for CosmosDB.
type CosmosDBBinding struct {
	DatabaseName  string `json:"databaseName"`
	ContainerName string `json:"containerName"`
	Connection    string `json:"connection"`
}

// HTTPBinding is the JSON representation for HTTP.
type HTTPBinding struct {
	AuthLevel string   `json:"authLevel"`
	Methods   []string `json:"methods"`
	Route     string   `json:"route,omitempty"`
}

// CosmosDocument represents a document in CosmosDB.
type CosmosDocument struct {
	ID          string `json:"id"`
	Data        string `json:"data"`
	Rid         string `json:"_rid"`
	Self        string `json:"_self"`
	Etag        string `json:"_etag"`
	Attachments string `json:"_attachments"`
	Timestamp   int64  `json:"_ts"`
	Lsn         int    `json:"_lsn"`
}

// CosmosDB is the user-facing configuration for a CosmosDB trigger.
type CosmosDB struct {
	ArgName       string
	DatabaseName  string
	ContainerName string
	Connection    string
}

func (c *CosmosDB) GetBindingType() BindingType { return CosmosDBBindingType }

func (c *CosmosDB) ToBinding() Binding {
	return Binding{
		Name:      c.ArgName,
		Type:      string(c.GetBindingType()),
		Direction: "in",
		CosmosDBBinding: &CosmosDBBinding{
			DatabaseName:  c.DatabaseName,
			ContainerName: c.ContainerName,
			Connection:    c.Connection,
		},
	}
}

func DeserializeCosmosDocument(jsonString string) []CosmosDocument {
	var docs []CosmosDocument
	json.Unmarshal([]byte(jsonString), &docs)
	return docs
}

// HttpTrigger is the user-facing configuration for an HTTP trigger.
type HttpTrigger struct {
	Name      string
	Route     string
	Methods   []string
	AuthLevel string // "anonymous", "function", "admin"
}

func (c *HttpTrigger) GetBindingType() BindingType { return HttpBindingType }

func (c *HttpTrigger) ToBinding() Binding {
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
