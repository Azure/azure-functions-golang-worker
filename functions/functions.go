package functions

import (
	"fmt"
	"reflect"
	"sync"
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
	TriggerMetadata map[string]string        // Optional trigger metadata
}

type FunctionRegistry struct {
	functions sync.Map
}

func (fr *FunctionRegistry) getFunction(functionId string) (*FunctionInfo, error) {
	funcInfoVal, found := fr.functions.Load(functionId)
	if !found {
		return nil, fmt.Errorf("function with ID %s not found", functionId)
	}

	funcInfo, ok := funcInfoVal.(*FunctionInfo)
	if !ok {
		return nil, fmt.Errorf("failed to cast FunctionInfo for ID %s", functionId)
	}

	return funcInfo, nil
}

func resolveTrigger(f interface{}, t Trigger, authLevel AuthorizationLevel) *FunctionInfo {
	switch tr := t.(type) {
	case *CosmosDBTrigger:
		return RegisterCosmosDBFunction(f, tr.ArgName, tr.ContainerName, tr.DatabaseName, tr.Connection)
	case *HttpTrigger:
		return RegisterHttpFunction(f, tr.Route, authLevel)
	default:
		return nil
	}
}

// func (disp *Dispatcher) RegisterCosmosFunction(f interface{}, argName string,
// 	containerName string, databaseName string, connection string) error {
// 	fi := getFunctionInfo(f, argName, containerName, databaseName, connection)
// 	fr := disp.FunctionRegistry

// 	funcId := fi.FunctionID
// 	if _, exists := fr.functions.Load(funcId); exists {
// 		return fmt.Errorf("function with ID %q already registered", funcId)
// 	}
// 	fr.functions.Store(funcId, fi)

// 	return nil
// }

func (disp *Dispatcher) RegisterFunction(f interface{}, t Trigger) error {
	fi := resolveTrigger(f, t, *disp.AuthLevel)
	fr := disp.FunctionRegistry

	funcId := fi.FunctionID
	if _, exists := fr.functions.Load(funcId); exists {
		return fmt.Errorf("function with ID %q already registered", funcId)
	}
	fr.functions.Store(funcId, fi)

	return nil
}
