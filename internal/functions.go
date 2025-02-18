package internal

import (
	"fmt"
	"reflect"
	"sync"

	functionrpc "github.com/azure/azure-functions-golang-worker/proto"
)

type ParamTypeInfo struct {
	BindingName string
	ParamType   reflect.Type
}

type FunctionInfo struct {
	Func            interface{}              // Function handler
	Name            string                   // Function name
	Directory       string                   // Function directory
	FunctionID      string                   // Unique function identifier
	HasReturn       bool                     // Whether the function has a return value
	IsHTTPFunc      bool                     // Whether the function is an HTTP function
	InputTypes      map[string]ParamTypeInfo // Mapping of input types
	OutputTypes     map[string]ParamTypeInfo // Mapping of output types
	ReturnType      *ParamTypeInfo           // Optional return type
	TriggerMetadata map[string]interface{}   // Optional trigger metadata
}

// FunctionRegistry holds metadata about loaded functions in memory.
type FunctionRegistry struct {
	mu        sync.RWMutex
	functions map[string]*FunctionInfo
}

var (
	registryInstance *FunctionRegistry
	once             sync.Once
)

func GetFunctionRegistry() *FunctionRegistry {
	once.Do(func() {
		registryInstance = &FunctionRegistry{
			functions: make(map[string]*FunctionInfo),
		}
	})
	return registryInstance
}

// Convert rpc metadata to function info to store in registry
// Will be used when host sends us actual request to parse info
// and cast symbols for cx code
func getFunctionInfo(function interface{}, metadata *functionrpc.RpcFunctionMetadata) *FunctionInfo {
	// Convert metadata to function info
	funcInfo := &FunctionInfo{
		Name:            metadata.Name,
		Directory:       metadata.Directory,
		FunctionID:      metadata.FunctionId,
		HasReturn:       false,
		IsHTTPFunc:      false,
		InputTypes:      make(map[string]ParamTypeInfo),
		OutputTypes:     make(map[string]ParamTypeInfo),
		TriggerMetadata: make(map[string]interface{}),
	}

	// Set the function handler
	funcInfo.Func = function

	return funcInfo
}

// // NewFunctionRegistry initializes and returns an empty function registry.
// func NewFunctionRegistry() *FunctionRegistry {
// 	return &FunctionRegistry{
// 		functions: make(map[string]*FunctionInfo),
// 	}
// }

// Generic RegisterFunction stores the given metadata under the specified function ID.
func RegisterFunction(funcID string, metadata *FunctionInfo) error {
	fr := GetFunctionRegistry()

	fr.mu.Lock()
	defer fr.mu.Unlock()

	if _, exists := fr.functions[funcID]; exists {
		return fmt.Errorf("function with ID %q already registered", funcID)
	}
	fr.functions[funcID] = metadata
	return nil
}

// RegisterBlobFunction stores the given metadata under the specified function ID for Blobs only
// To have more control over inputs and outputs, we can have type specific functions that the
// cx can use
// Temporarily passing the function and metadata separate - extract for translation
func RegisterBlobFunction(function interface{}, funcID string, metadata *functionrpc.RpcFunctionMetadata) error {
	fi := getFunctionInfo(function, metadata)
	fr := GetFunctionRegistry()

	fr.mu.Lock()
	defer fr.mu.Unlock()

	if _, exists := fr.functions[funcID]; exists {
		return fmt.Errorf("function with ID %q already registered", funcID)
	}
	fr.functions[funcID] = fi
	return nil
}

// GetFunction retrieves the metadata for the function with the given ID.
// Returns nil and false if it does not exist.
func GetFunction(funcID string) (*FunctionInfo, bool) {
	fr := GetFunctionRegistry()

	fr.mu.RLock()
	defer fr.mu.RUnlock()

	meta, ok := fr.functions[funcID]
	return meta, ok
}
