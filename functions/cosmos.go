package functions

import (
	"encoding/json"
)

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

type CosmosDB struct {
	ArgName       string
	DatabaseName  string
	ContainerName string
	Connection    string
}

type CosmosDBBinding struct {
	DatabaseName  string `json:"databaseName"`
	ContainerName string `json:"containerName"`
	Connection    string `json:"connection"`
}

func (c *CosmosDB) GetBindingType() BindingType { return CosmosDBFunction }

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
