package sdk

import "sync"

// ClientFactory is a function that creates a trigger-specific client argument
// from the binding configuration and trigger metadata.
// Used by trigger extension modules (e.g., triggers/blob) to create SDK clients
// (e.g., *blob.Client) during invocation without coupling the core SDK to
// external Azure SDK dependencies.
type ClientFactory func(config map[string]any, triggerMetadata map[string]string) (any, error)

var (
	clientFactories   = make(map[string]ClientFactory)
	clientFactoriesMu sync.RWMutex
)

// RegisterClientFactory registers a ClientFactory for a trigger type.
// This is typically called by extension packages in their init() functions,
// similar to how database/sql drivers are registered.
//
// Example:
//
//	func init() {
//	    sdk.RegisterClientFactory("blobTrigger", createBlobClient)
//	}
func RegisterClientFactory(triggerType string, factory ClientFactory) {
	clientFactoriesMu.Lock()
	defer clientFactoriesMu.Unlock()
	clientFactories[triggerType] = factory
}

// GetClientFactory retrieves a registered ClientFactory for a trigger type.
func GetClientFactory(triggerType string) (ClientFactory, bool) {
	clientFactoriesMu.RLock()
	defer clientFactoriesMu.RUnlock()
	fn, ok := clientFactories[triggerType]
	return fn, ok
}
