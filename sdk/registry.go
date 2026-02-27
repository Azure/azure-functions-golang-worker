package sdk

import (
	"context"
	"reflect"
	"sync"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

// ConverterFunc defines the signature for custom type converters.
// It takes the Binding Config, input TypedData, and Trigger Metadata.
// It returns a reflect.Value containing the constructed object (e.g. *azblob.Client).
type ConverterFunc func(ctx context.Context, config map[string]interface{}, data *pb.TypedData, metadata map[string]*pb.TypedData) (reflect.Value, error)

var (
	// CustomConverters maps a reflect.Type to a specific converter function.
	customConverters = make(map[reflect.Type]ConverterFunc)
	converterMu      sync.RWMutex
)

// RegisterConverter registers a custom converter for a specific target type.
// This is typically called by extension packages in their init() methods.
func RegisterConverter(targetType interface{}, fn ConverterFunc) {
	converterMu.Lock()
	defer converterMu.Unlock()
	customConverters[reflect.TypeOf(targetType)] = fn
}

// GetConverter retrieves a registered converter for a type.
func GetConverter(targetType reflect.Type) (ConverterFunc, bool) {
	converterMu.RLock()
	defer converterMu.RUnlock()

	// Direct match
	fn, found := customConverters[targetType]
	if found {
		return fn, true
	}

	// Pointer check: If targetType is *Service and registry has *Service, it matches.
	// If targetType is *Service and registry has Service, we might handle... but usually registry has the exact type users ask for.
	// Let's defer strict matching to the caller or strict registration.

	// However, if we register (*azblob.Client)(nil), reflect.TypeOf gives *azblob.Client.
	// If the user function asks for *azblob.Client, it matches.

	return nil, false
}
