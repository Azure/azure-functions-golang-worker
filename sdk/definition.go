package sdk

import "github.com/azure/azure-functions-golang-worker/sdk/bindings"

// FunctionDefinition represents a loaded function's metadata.
// This is used by the worker to execute the function.
type FunctionDefinition struct {
	FuncId         string
	FuncName       string
	InputBindings  map[string]GrpcBindingMetadata
	RegisteredFunc RegisteredFunction
}

// GrpcBindingMetadata is a copy of what the worker needs.
type GrpcBindingMetadata struct {
	Name      string
	Type      string
	Direction bindings.BindingDirection
}
