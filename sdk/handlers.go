package sdk

import (
	"context"
	"net/http"

	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
)

// ---------------------------------------------------------------------------
// Core Trigger Handler Types
//
// These typed aliases exist for Core Triggers — triggers whose payload is
// bounded, already serialized in the gRPC InvocationRequest, and requires
// only encoding/json for deserialization (no external Azure SDK).
//
// Typed aliases provide compile-time safety: the compiler catches wrong
// handler signatures immediately, unlike Extension Triggers which accept
// 'any' and validate via reflection at registration time.
//
// Extension Triggers (e.g., Blob) do NOT have typed aliases here because
// their handler's second argument is an SDK client type (e.g., *blob.Client)
// that lives in an external module. Adding it here would couple the core SDK
// to azblob/azidentity and bloat every user's binary.
// ---------------------------------------------------------------------------

// HTTPHandler is the handler type for HTTP triggered functions.
// It uses the standard net/http handler signature for maximum compatibility
// with the Go ecosystem (middleware, testing, etc.).
type HTTPHandler = http.HandlerFunc

// TimerHandler is the handler type for timer triggered functions.
type TimerHandler = func(context.Context, bindings.TimerInfo) error

// ServiceBusHandler is the handler type for Service Bus triggered functions
// (both queue and topic triggers).
type ServiceBusHandler = func(context.Context, bindings.ServiceBusMessage) error

// ServiceBusBatchHandler is the handler type for batched Service Bus triggered
// functions (both queue and topic triggers).
type ServiceBusBatchHandler = func(context.Context, []bindings.ServiceBusMessage) error

// EventHubHandler is the handler type for Event Hub triggered functions.
type EventHubHandler = func(context.Context, bindings.EventHubMessage) error

// EventHubBatchHandler is the handler type for batched Event Hub triggered functions.
type EventHubBatchHandler = func(context.Context, []bindings.EventHubMessage) error

// CosmosDBHandler is the handler type for CosmosDB triggered functions.
type CosmosDBHandler = func(context.Context, []bindings.CosmosDocument) error

// SQLChangeHandler is the handler type for SQL triggered functions.
// It receives a batch of row changes captured via SQL Change Tracking;
// each SQLChange carries an Operation and the row data as raw JSON.
type SQLChangeHandler = func(context.Context, []bindings.SQLChange) error

// EventGridHandler is the handler type for Event Grid triggered functions.
type EventGridHandler = func(context.Context, bindings.EventGridEvent) error

// QueueHandler is the handler type for Azure Storage Queue triggered functions.
type QueueHandler = func(context.Context, bindings.QueueMessage) error

// BlobHandler is the handler type for blob triggered functions that receive
// the blob content as raw bytes. For large blobs or SDK-type blob client
// access, use the triggers/blob module instead.
type BlobHandler = func(context.Context, []byte) error
