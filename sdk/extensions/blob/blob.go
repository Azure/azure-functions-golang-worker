package blob

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

func init() {
	// Register *azblob.Client (Service Client)
	sdk.RegisterConverter((*azblob.Client)(nil), convertToServiceClient)
	// Register *blob.Client (Specific Blob Client)
	sdk.RegisterConverter((*blob.Client)(nil), convertToBlobClient)
}

func convertToBlobClient(ctx context.Context, config map[string]interface{}, data *pb.TypedData, metadata map[string]*pb.TypedData) (reflect.Value, error) {
	connSetting, _ := config["connection"].(string)
	if connSetting == "" {
		connSetting = "AzureWebJobsStorage"
	}
	connStr := os.Getenv(connSetting)
	if connStr == "" {
		return reflect.Value{}, fmt.Errorf("connection string environment variable '%s' not found", connSetting)
	}

	// Resolve Path
	// 1. Get the path template from config
	pathTemplate, _ := config["path"].(string)
	resolvedPath := pathTemplate

	// 2. Resolve parameters using Trigger Metadata
	// e.g. path="samples/{name}", metadata["name"]="file1" -> "samples/file1"
	for k, v := range metadata {
		val := v.GetString_()
		if val != "" {
			token := "{" + k + "}"
			resolvedPath = strings.ReplaceAll(resolvedPath, token, val)
		}
	}

	// 3. Special handling for BlobTrigger:
	// If this binding IS the trigger, the host sends the actual path in "BlobTrigger" metadata.
	// How do we know if this is the trigger? We check if generic 'BlobTrigger' metadata exists and matches?
	// Simpler: If "BlobTrigger" metadata is present, AND the resolved path seems empty or generic, use it?
	// Actually, for the Trigger binding itself, config["path"] is the pattern.
	// We can try to use "BlobTrigger" metadata if available as authoritative source for the trigger.

	// However, since we don't have "BindingType" in config map (unless we add it in handlers.go),
	// let's rely on the resolution loop above which works for Input Bindings.
	// For Trigger bindings, the Host populates metadata with the captured groups (e.g. {name}), so resolution works there too.
	// EXCEPT: if the pattern is just "container/blob.txt", no resolution needed.

	// Fix: If resolvedPath is still empty (shouldn't be), error.
	if resolvedPath == "" {
		// Fallback for BlobTrigger: use metadata["BlobTrigger"]
		if v, ok := metadata["BlobTrigger"]; ok {
			resolvedPath = v.GetString_()
		}
	}

	if resolvedPath == "" {
		return reflect.Value{}, fmt.Errorf("unable to resolve blob path")
	}

	// Create Service Client
	service, err := azblob.NewClientFromConnectionString(connStr, nil)
	if err != nil {
		return reflect.Value{}, fmt.Errorf("failed to create blob client: %v", err)
	}

	// Split Container/Blob
	parts := strings.SplitN(resolvedPath, "/", 2)
	if len(parts) < 2 {
		return reflect.Value{}, fmt.Errorf("invalid blob path: '%s'. Expected 'container/blob'", resolvedPath)
	}
	containerName := parts[0]
	blobName := parts[1]

	// Create Blob Client
	// azblob v1.0.0+: Service -> NewContainerClient -> NewBlobClient
	containerClient := service.ServiceClient().NewContainerClient(containerName)
	blobClient := containerClient.NewBlobClient(blobName)

	return reflect.ValueOf(blobClient), nil
}

func convertToServiceClient(ctx context.Context, config map[string]interface{}, data *pb.TypedData, metadata map[string]*pb.TypedData) (reflect.Value, error) {
	connSetting, _ := config["connection"].(string)
	if connSetting == "" {
		// Fallback to "AzureWebJobsStorage" if empty? Or error?
		connSetting = "AzureWebJobsStorage"
	}
	connStr := os.Getenv(connSetting)
	if connStr == "" {
		return reflect.Value{}, fmt.Errorf("connection string environment variable '%s' not found", connSetting)
	}

	// Create Service Client
	client, err := azblob.NewClientFromConnectionString(connStr, nil)
	if err != nil {
		return reflect.Value{}, fmt.Errorf("failed to create blob client: %v", err)
	}

	return reflect.ValueOf(client), nil
}

func resolveConfig(config map[string]interface{}, metadata map[string]*pb.TypedData) (string, string, error) {
	// Helper to get connection and resolve path
	return "", "", nil
}
