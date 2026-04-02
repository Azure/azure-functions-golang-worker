// Package blobtrigger provides blob trigger support for Azure Functions Go Worker.
//
// Import this package with a blank identifier to enable blob trigger functions
// that receive a *blob.Client scoped to the specific blob that triggered:
//
//	import _ "github.com/azure/azure-functions-golang-worker/triggers/blob"
//
// Then use app.Blob() to register handlers:
//
//	app.Blob("blobFunc", myHandler).Path("container/{name}").Connection("AzureWebJobsStorage")
//
// The handler receives a *blob.Client from the Azure SDK:
//
//	func myHandler(ctx context.Context, client *blob.Client) error {
//	    data, _ := client.DownloadStream(ctx, nil)
//	    // ...
//	    return nil
//	}
//
// Both connection string and managed identity auth are supported. The auth
// method is auto-detected from the environment variable value.
package blobtrigger

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/azure/azure-functions-golang-worker/sdk"
)

func init() {
	sdk.RegisterClientFactory("blobTrigger", createBlobClientFromTrigger)
}

// createBlobClientFromTrigger is the ClientFactory for blob triggers.
// It reads the connection setting and blob path from the binding config
// and trigger metadata, then creates a *blob.Client for the specific blob.
func createBlobClientFromTrigger(config map[string]any, triggerMeta map[string]string) (any, error) {
	// Get connection setting name from binding config
	connection, _ := config["connection"].(string)
	if connection == "" {
		connection = "AzureWebJobsStorage"
	}

	// Get path template from binding config
	path, _ := config["path"].(string)

	// Resolve path parameters from trigger metadata
	for k, v := range triggerMeta {
		path = strings.ReplaceAll(path, "{"+k+"}", v)
	}
	// Fallback: use BlobTrigger metadata for the actual resolved path
	if bt, ok := triggerMeta["BlobTrigger"]; ok && bt != "" {
		path = bt
	}

	if path == "" {
		return nil, fmt.Errorf("unable to resolve blob path from trigger metadata")
	}

	return CreateBlobClient(connection, path)
}

// --- Client creation with caching and auto-detect auth ---

var (
	serviceClients   = make(map[string]*azblob.Client)
	serviceClientsMu sync.RWMutex
)

// createServiceClient creates a new azblob.Client, auto-detecting auth method.
// If the env var value contains "AccountName=" or "DefaultEndpointsProtocol=",
// it's treated as a connection string. Otherwise, it's treated as a storage
// account URL and DefaultAzureCredential is used.
func createServiceClient(connectionSetting string) (*azblob.Client, error) {
	value := os.Getenv(connectionSetting)
	if value == "" {
		return nil, fmt.Errorf("environment variable %q not set", connectionSetting)
	}

	// Connection string auth
	if strings.Contains(value, "AccountName=") || strings.Contains(value, "DefaultEndpointsProtocol=") {
		return azblob.NewClientFromConnectionString(value, nil)
	}

	// Identity-based auth (URL + DefaultAzureCredential)
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create credential: %v", err)
	}
	return azblob.NewClient(value, cred, nil)
}

// GetOrCreateServiceClient returns a cached service client for the given
// connection setting, creating one if it doesn't exist.
func GetOrCreateServiceClient(connectionSetting string) (*azblob.Client, error) {
	serviceClientsMu.RLock()
	client, ok := serviceClients[connectionSetting]
	serviceClientsMu.RUnlock()
	if ok {
		return client, nil
	}

	serviceClientsMu.Lock()
	defer serviceClientsMu.Unlock()

	// Double-check after acquiring write lock
	if client, ok := serviceClients[connectionSetting]; ok {
		return client, nil
	}

	client, err := createServiceClient(connectionSetting)
	if err != nil {
		return nil, err
	}
	serviceClients[connectionSetting] = client
	return client, nil
}

// CreateBlobClient creates a *blob.Client for a specific blob path using
// a cached service client. The path should be "container/blobname".
func CreateBlobClient(connectionSetting, resolvedPath string) (*blob.Client, error) {
	service, err := GetOrCreateServiceClient(connectionSetting)
	if err != nil {
		return nil, err
	}

	parts := strings.SplitN(resolvedPath, "/", 2)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid blob path %q: expected 'container/blob'", resolvedPath)
	}

	containerClient := service.ServiceClient().NewContainerClient(parts[0])
	blobClient := containerClient.NewBlobClient(parts[1])

	return blobClient, nil
}
