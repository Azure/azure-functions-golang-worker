package bindings

const CosmosDBBindingType BindingType = "cosmosDBTrigger"

// CosmosDBBinding is the JSON representation for CosmosDB.
type CosmosDBBinding struct {
	DatabaseName  string `json:"databaseName"`
	ContainerName string `json:"containerName"`
	Connection    string `json:"connection"`
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

// CosmosDBTrigger is the user-facing configuration for a CosmosDB trigger.
type CosmosDBTrigger struct {
	Name          string
	DatabaseName  string
	ContainerName string
	Connection    string
}

func (c *CosmosDBTrigger) GetBindingType() BindingType { return CosmosDBBindingType }

func (c *CosmosDBTrigger) ToBinding() Binding {
	return Binding{
		Name:      c.Name,
		Type:      string(c.GetBindingType()),
		Direction: "in",
		CosmosDBBinding: &CosmosDBBinding{
			DatabaseName:  c.DatabaseName,
			ContainerName: c.ContainerName,
			Connection:    c.Connection,
		},
	}
}

