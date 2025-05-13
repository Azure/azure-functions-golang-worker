package functions

import (
	"encoding/json"
	"reflect"
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

type CosmosDBTrigger struct {
	ArgName       string
	ContainerName string
	DatabaseName  string
	Connection    string
}

func (c *CosmosDBTrigger) GetTriggerType() TriggerType { return CosmosDB }

func RegisterCosmosDBFunction(f interface{}, argName string, containerName string, databaseName string, connection string) *FunctionInfo {
	inputTypes := make(map[string]ParamTypeInfo)
	inputTypes[argName] = ParamTypeInfo{
		BindingName: "cosmosDBTrigger",
		ParamType:   reflect.TypeOf([]CosmosDocument{}),
	}

	triggerMetadata := make(map[string]string)
	triggerMetadata["direction"] = "IN"
	triggerMetadata["type"] = "cosmosDBTrigger"
	triggerMetadata["name"] = argName
	triggerMetadata["connection"] = connection
	triggerMetadata["databaseName"] = databaseName
	triggerMetadata["containerName"] = containerName

	return &FunctionInfo{
		Func:            f,
		Name:            "CosmosDBTrigger",
		Directory:       "Dir",
		FunctionID:      "0f7b4505-98b8-4bd2-b71a-3ec427bd4c58",
		HasReturn:       false,
		IsHTTPFunc:      false,
		InputTypes:      inputTypes,
		OutputTypes:     make(map[string]ParamTypeInfo),
		TriggerMetadata: triggerMetadata,
	}
}

func DeserializeCosmosDocument(jsonString string) []CosmosDocument {
	var docs []CosmosDocument
	json.Unmarshal([]byte(jsonString), &docs)

	return docs
}
