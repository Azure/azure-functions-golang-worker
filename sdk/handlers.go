package sdk

import (
	"context"
	"net/http"

	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
)

// HTTPHandler is the handler type for HTTP triggered functions.
// It uses the standard net/http handler signature for maximum compatibility
// with the Go ecosystem (middleware, testing, etc.).
type HTTPHandler = http.HandlerFunc

// TimerHandler is the handler type for timer triggered functions.
type TimerHandler = func(context.Context, bindings.TimerInfo) error

// ServiceBusHandler is the handler type for Service Bus triggered functions
// (both queue and topic triggers).
type ServiceBusHandler = func(context.Context, bindings.ServiceBusMessage) error

// EventHubHandler is the handler type for Event Hub triggered functions.
type EventHubHandler = func(context.Context, bindings.EventHubMessage) error

// CosmosDBHandler is the handler type for CosmosDB triggered functions.
type CosmosDBHandler = func(context.Context, []bindings.CosmosDocument) error

// EventGridHandler is the handler type for Event Grid triggered functions.
type EventGridHandler = func(context.Context, bindings.EventGridEvent) error

// BlobHandler is the handler type for blob triggered functions that receive
// the blob content as raw bytes. For large blobs or SDK-type blob client
// access, use the triggers/blob module instead.
type BlobHandler = func(context.Context, []byte) error
