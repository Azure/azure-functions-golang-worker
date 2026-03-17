package eventhub

import (
	"context"
	"fmt"
	"os"
	"reflect"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

func init() {
	// Register *azeventhubs.ConsumerClient for trigger/input bindings
	sdk.RegisterConverter((*azeventhubs.ConsumerClient)(nil), convertToConsumerClient)
	// Register *azeventhubs.ProducerClient for output bindings
	sdk.RegisterConverter((*azeventhubs.ProducerClient)(nil), convertToProducerClient)
}

func convertToConsumerClient(ctx context.Context, config map[string]interface{}, data *pb.TypedData, metadata map[string]*pb.TypedData) (reflect.Value, error) {
	connSetting, _ := config["connection"].(string)
	if connSetting == "" {
		connSetting = "EventHubConnection"
	}
	connStr := os.Getenv(connSetting)
	if connStr == "" {
		return reflect.Value{}, fmt.Errorf("connection string environment variable '%s' not found", connSetting)
	}

	eventHubName, _ := config["eventHubName"].(string)
	if eventHubName == "" {
		return reflect.Value{}, fmt.Errorf("eventHubName is required in binding configuration")
	}

	consumerGroup, _ := config["consumerGroup"].(string)
	if consumerGroup == "" {
		consumerGroup = azeventhubs.DefaultConsumerGroup
	}

	client, err := azeventhubs.NewConsumerClientFromConnectionString(connStr, eventHubName, consumerGroup, nil)
	if err != nil {
		return reflect.Value{}, fmt.Errorf("failed to create EventHub consumer client: %v", err)
	}

	return reflect.ValueOf(client), nil
}

func convertToProducerClient(ctx context.Context, config map[string]interface{}, data *pb.TypedData, metadata map[string]*pb.TypedData) (reflect.Value, error) {
	connSetting, _ := config["connection"].(string)
	if connSetting == "" {
		connSetting = "EventHubConnection"
	}
	connStr := os.Getenv(connSetting)
	if connStr == "" {
		return reflect.Value{}, fmt.Errorf("connection string environment variable '%s' not found", connSetting)
	}

	eventHubName, _ := config["eventHubName"].(string)
	if eventHubName == "" {
		return reflect.Value{}, fmt.Errorf("eventHubName is required in binding configuration")
	}

	client, err := azeventhubs.NewProducerClientFromConnectionString(connStr, eventHubName, nil)
	if err != nil {
		return reflect.Value{}, fmt.Errorf("failed to create EventHub producer client: %v", err)
	}

	return reflect.ValueOf(client), nil
}
