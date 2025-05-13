package functions

type TriggerType string

const (
	CosmosDB TriggerType = "CosmosDBTrigger"
	Http     TriggerType = "HttpTrigger"
	Blob     TriggerType = "BlobTrigger"
	Queue    TriggerType = "QueueTrigger"
)

type Trigger interface {
	GetTriggerType() TriggerType
}
