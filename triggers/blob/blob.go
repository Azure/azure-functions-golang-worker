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
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/azure/azure-functions-golang-worker/sdk"
)

// BlobHandler is the handler type for blob triggered functions.
// The *blob.Client is scoped to the specific blob that triggered the function.
type BlobHandler = func(context.Context, *blob.Client) error

func init() {
	sdk.RegisterClientFactory("blobTrigger", createBlobClientFromTrigger)
}

// Register creates a new blob-triggered function on the given app.
// This is a convenience wrapper around app.Blob() for use by the triggers/blob module.
func Register(app *sdk.App, name string, f BlobHandler, opts ...sdk.Option) *sdk.RegisteredFunction {
	return app.Blob(name, f, opts...)
}

// WithPath sets the blob path pattern (e.g., "container/{name}").
func WithPath(path string) sdk.Option {
	return func(rf *sdk.RegisteredFunction) {
		if b := rf.TriggerBinding(); b != nil && b.BlobBinding != nil {
			b.BlobBinding.Path = path
		}
	}
}

// WithBlobConnection sets the connection string setting name (e.g., "AzureWebJobsStorage").
func WithBlobConnection(connection string) sdk.Option {
	return func(rf *sdk.RegisteredFunction) {
		if b := rf.TriggerBinding(); b != nil && b.BlobBinding != nil {
			b.BlobBinding.Connection = connection
		}
	}
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
//
// Auth detection follows the Azure Functions host convention:
//
//  1. Connection string: If the env var value contains "AccountKey=",
//     "DefaultEndpointsProtocol=", or "UseDevelopmentStorage=true", it's
//     treated as a connection string.
//
//  2. Identity-based (managed identity): If the env var is empty or not a
//     connection string, the function checks for __-suffixed env vars:
//     - {connection}__accountName   → builds https://{name}.blob.core.windows.net
//     - {connection}__blobServiceUri → uses as-is
//     - {connection}__serviceUri     → uses as-is (fallback)
//     - {connection}__clientId       → user-assigned managed identity client ID
//
//  3. Direct URL: If the env var value is a URL (not a connection string),
//     it's used as the endpoint with DefaultAzureCredential.
func createServiceClient(connectionSetting string) (*azblob.Client, error) {
	value := os.Getenv(connectionSetting)

	// 1. Connection string auth
	if value != "" && isConnectionString(value) {
		return azblob.NewClientFromConnectionString(value, nil)
	}

	// 2. If value is a URL (not empty, not connection string), use it directly
	if value != "" {
		cred, err := buildCredential(connectionSetting)
		if err != nil {
			return nil, err
		}
		return azblob.NewClient(value, cred, nil)
	}

	// 3. Identity-based auth via __-suffixed env vars
	endpoint := resolveEndpoint(connectionSetting)
	if endpoint == "" {
		return nil, fmt.Errorf(
			"environment variable %q is empty and no %s__accountName, %s__blobServiceUri, or %s__serviceUri found",
			connectionSetting, connectionSetting, connectionSetting, connectionSetting,
		)
	}

	cred, err := buildCredential(connectionSetting)
	if err != nil {
		return nil, err
	}
	return azblob.NewClient(endpoint, cred, nil)
}

// isConnectionString checks if a value looks like an Azure Storage connection string.
func isConnectionString(val string) bool {
	return strings.Contains(val, "AccountKey=") ||
		strings.Contains(val, "DefaultEndpointsProtocol=") ||
		strings.Contains(val, "UseDevelopmentStorage=true")
}

// resolveEndpoint resolves the blob service endpoint from __-suffixed env vars.
// Returns empty string if none are found.
func resolveEndpoint(connectionSetting string) string {
	// {connection}__accountName → https://{name}.blob.core.windows.net
	if accountName := os.Getenv(connectionSetting + "__accountName"); accountName != "" {
		return fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
	}

	// {connection}__blobServiceUri → use as-is
	if blobServiceURI := os.Getenv(connectionSetting + "__blobServiceUri"); blobServiceURI != "" {
		return blobServiceURI
	}

	// {connection}__serviceUri → use as-is (fallback)
	if serviceURI := os.Getenv(connectionSetting + "__serviceUri"); serviceURI != "" {
		return serviceURI
	}

	return ""
}

// buildCredential creates an Azure credential for identity-based auth.
// If {connection}__clientId is set, it creates a ManagedIdentityCredential
// with the specified client ID (for user-assigned managed identity).
// Otherwise, it creates a DefaultAzureCredential which tries multiple
// auth methods (managed identity, Azure CLI, environment variables, etc.).
func buildCredential(connectionSetting string) (azcore.TokenCredential, error) {
	clientID := os.Getenv(connectionSetting + "__clientId")

	if clientID != "" {
		// User-assigned managed identity
		cred, err := azidentity.NewManagedIdentityCredential(&azidentity.ManagedIdentityCredentialOptions{
			ID: azidentity.ClientID(clientID),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create ManagedIdentityCredential with clientId %q: %v", clientID, err)
		}
		return cred, nil
	}

	// Default credential chain (system-assigned MI, Azure CLI, env vars, etc.)
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create DefaultAzureCredential: %v", err)
	}
	return cred, nil
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
