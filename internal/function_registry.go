package internal

import (
	"fmt"
	"sync"

	functionrpc "github.com/azure/azure-functions-golang-worker/proto"
)

// FunctionRegistry holds metadata about loaded functions in memory.
type FunctionRegistry struct {
	mu        sync.RWMutex
	functions map[string]*functionrpc.RpcFunctionMetadata
}

// NewFunctionRegistry initializes and returns an empty function registry.
func NewFunctionRegistry() *FunctionRegistry {
	return &FunctionRegistry{
		functions: make(map[string]*functionrpc.RpcFunctionMetadata),
	}
}

// RegisterFunction stores the given metadata under the specified function ID.
func (fr *FunctionRegistry) RegisterFunction(funcID string, metadata *functionrpc.RpcFunctionMetadata) error {
	fr.mu.Lock()
	defer fr.mu.Unlock()

	if _, exists := fr.functions[funcID]; exists {
		return fmt.Errorf("function with ID %q already registered", funcID)
	}
	fr.functions[funcID] = metadata
	return nil
}

// GetFunction retrieves the metadata for the function with the given ID.
// Returns nil and false if it does not exist.
func (fr *FunctionRegistry) GetFunction(funcID string) (*functionrpc.RpcFunctionMetadata, bool) {
	fr.mu.RLock()
	defer fr.mu.RUnlock()

	meta, ok := fr.functions[funcID]
	return meta, ok
}
