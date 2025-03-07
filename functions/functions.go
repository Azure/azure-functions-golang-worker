package functions

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/azure/azure-functions-golang-worker/sdk"
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

func getFunctionInfo(f interface{}, argName string, containerName string, databaseName string, connection string) *FunctionInfo {
	inputTypes := make(map[string]ParamTypeInfo)
	inputTypes[argName] = ParamTypeInfo{
		BindingName: "cosmosDBTrigger",
		ParamType:   reflect.TypeOf([]sdk.CosmosDocument{}),
	}

	triggerMetadata := make(map[string]string)
	triggerMetadata["direction"] = "IN"
	triggerMetadata["type"] = "cosmosDBTrigger"
	triggerMetadata["name"] = argName
	triggerMetadata["connection"] = connection
	triggerMetadata["databaseName"] = databaseName
	triggerMetadata["containerName"] = containerName

	return &FunctionInfo{
		Func:            f,
		Name:            "CosmosDBTrigger",
		Directory:       "Dir",
		FunctionID:      "0f7b4505-98b8-4bd2-b71a-3ec427bd4c58",
		HasReturn:       false,
		IsHTTPFunc:      false,
		InputTypes:      inputTypes,
		OutputTypes:     make(map[string]ParamTypeInfo),
		TriggerMetadata: triggerMetadata,
	}
}

func (disp *Dispatcher) RegisterCosmosFunction(f interface{}, argName string,
	containerName string, databaseName string, connection string) error {
	fi := getFunctionInfo(f, argName, containerName, databaseName, connection)
	fr := disp.FunctionRegistry

	funcId := fi.FunctionID
	if _, exists := fr.functions.Load(funcId); exists {
		return fmt.Errorf("function with ID %q already registered", funcId)
	}
	fr.functions.Store(funcId, fi)

	//GetFunctionDetails(f)

	return nil
}
