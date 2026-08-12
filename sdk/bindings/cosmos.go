package bindings

import "encoding/json"

// CosmosDBTrigger is the binding type constant for CosmosDB triggers.
const CosmosDBTriggerType BindingType = "cosmosDBTrigger"

// CosmosDBBinding is the JSON representation for CosmosDB.
type CosmosDBBinding struct {
	DatabaseName                    string `json:"databaseName"`
	ContainerName                   string `json:"containerName"`
	Connection                      string `json:"connection"`
	LeaseContainerName              string `json:"leaseContainerName,omitempty"`
	LeaseDatabaseName               string `json:"leaseDatabaseName,omitempty"`
	LeaseConnection                 string `json:"leaseConnection,omitempty"`
	CreateLeaseContainerIfNotExists bool   `json:"createLeaseContainerIfNotExists,omitempty"`
	LeasesContainerThroughput       int    `json:"leasesContainerThroughput,omitempty"`
	LeaseContainerPrefix            string `json:"leaseContainerPrefix,omitempty"`
	FeedPollDelay                   int    `json:"feedPollDelay,omitempty"`
	LeaseRenewInterval              int    `json:"leaseRenewInterval,omitempty"`
	LeaseAcquireInterval            int    `json:"leaseAcquireInterval,omitempty"`
	LeaseExpirationInterval         int    `json:"leaseExpirationInterval,omitempty"`
	MaxItemsPerInvocation           int    `json:"maxItemsPerInvocation,omitempty"`
	StartFromBeginning              bool   `json:"startFromBeginning,omitempty"`
	StartFromTime                   string `json:"startFromTime,omitempty"`
	PreferredLocations              string `json:"preferredLocations,omitempty"`
}

// CosmosDocument represents a document from the CosmosDB change feed.
// System properties: _rid, _self, _etag, _attachments, _ts (timestamp), _lsn.
// Use json.Unmarshal on the raw document to extract custom properties.
type CosmosDocument struct {
	ID          string          `json:"id"`
	Data        json.RawMessage `json:"data"`
	Rid         string          `json:"_rid"`
	Self        string          `json:"_self"`
	Etag        string          `json:"_etag"`
	Attachments string          `json:"_attachments"`
	Timestamp   int64           `json:"_ts"`
	Lsn         int             `json:"_lsn"`
}

// CosmosDBTrigger is the user-facing configuration for a CosmosDB trigger.
type CosmosDBTrigger struct {
	Name                            string
	DatabaseName                    string
	ContainerName                   string
	Connection                      string
	LeaseContainerName              string
	LeaseDatabaseName               string
	LeaseConnection                 string
	CreateLeaseContainerIfNotExists bool
	LeasesContainerThroughput       int
	LeaseContainerPrefix            string
	FeedPollDelay                   int
	LeaseRenewInterval              int
	LeaseAcquireInterval            int
	LeaseExpirationInterval         int
	MaxItemsPerInvocation           int
	StartFromBeginning              bool
	StartFromTime                   string
	PreferredLocations              string
}

func (c *CosmosDBTrigger) GetBindingType() BindingType { return CosmosDBTriggerType }

func (c *CosmosDBTrigger) ToBinding() Binding {
	return Binding{
		Name:      c.Name,
		Type:      string(c.GetBindingType()),
		Direction: "in",
		CosmosDBBinding: &CosmosDBBinding{
			DatabaseName:                    c.DatabaseName,
			ContainerName:                   c.ContainerName,
			Connection:                      c.Connection,
			LeaseContainerName:              c.LeaseContainerName,
			LeaseDatabaseName:               c.LeaseDatabaseName,
			LeaseConnection:                 c.LeaseConnection,
			CreateLeaseContainerIfNotExists: c.CreateLeaseContainerIfNotExists,
			LeasesContainerThroughput:       c.LeasesContainerThroughput,
			LeaseContainerPrefix:            c.LeaseContainerPrefix,
			FeedPollDelay:                   c.FeedPollDelay,
			LeaseRenewInterval:              c.LeaseRenewInterval,
			LeaseAcquireInterval:            c.LeaseAcquireInterval,
			LeaseExpirationInterval:         c.LeaseExpirationInterval,
			MaxItemsPerInvocation:           c.MaxItemsPerInvocation,
			StartFromBeginning:              c.StartFromBeginning,
			StartFromTime:                   c.StartFromTime,
			PreferredLocations:              c.PreferredLocations,
		},
	}
}
