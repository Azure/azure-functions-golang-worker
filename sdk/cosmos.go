package sdk

import "encoding/json"

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

func DeserializeCosmosDocument(jsonString string) []CosmosDocument {
	var docs []CosmosDocument
	json.Unmarshal([]byte(jsonString), &docs)

	return docs
}
