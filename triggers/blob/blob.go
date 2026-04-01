// Package blob provides a blob trigger for Azure Functions Go Worker.
// It creates a *blob.Client pointed at the specific blob that triggered
// the function, supporting both connection string and managed identity auth.
package blob

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
)

// BlobHandler is the handler type for blob triggered functions.
// The *blob.Client is scoped to the specific blob that triggered the function.
type BlobHandler = func(context.Context, *blob.Client) error

// BlobFunctionBuilder provides a fluent API for configuring blob-triggered functions.
type BlobFunctionBuilder struct {
	trigger *bindings.Blob
	rf      *sdk.RegisteredFunction
}

// Register creates a new blob-triggered function on the given app.
func Register(app *sdk.App, name string, f BlobHandler) *BlobFunctionBuilder {
	trigger := &bindings.Blob{
		Name: "blob",
	}

	rf := app.RegisterFunction(f, trigger)

	return &BlobFunctionBuilder{
		trigger: trigger,
		rf:      rf,
	}
}

// Path sets the blob path pattern (e.g., "container/{name}").
func (b *BlobFunctionBuilder) Path(path string) *BlobFunctionBuilder {
	b.trigger.Path = path
	b.updateBinding()
	return b
}

// Connection sets the connection string setting name (e.g., "AzureWebJobsStorage").
func (b *BlobFunctionBuilder) Connection(connection string) *BlobFunctionBuilder {
	b.trigger.Connection = connection
	b.updateBinding()
	return b
}

func (b *BlobFunctionBuilder) updateBinding() {
	if len(b.rf.RawBindings) > 0 {
		b.rf.RawBindings[0] = b.trigger.ToBinding()
	}
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
