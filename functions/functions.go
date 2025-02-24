package functions

import (
	"fmt"
	"reflect"
	"sync"

	pb "github.com/azure/azure-functions-golang-worker/proto"
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

func generateRPCMetadata() *pb.RpcFunctionMetadata {
	metadata := pb.RpcFunctionMetadata{
		Name:       "MyFunction",
		Directory:  "/home/user/functions/my_function",
		ScriptFile: "handler.go",
		EntryPoint: "main",
		Bindings: map[string]*pb.BindingInfo{
			"httpTrigger": { /* Initialize BindingInfo fields */ },
		},
		IsProxy:                  false,
		Status:                   &pb.StatusResult{ /* Initialize StatusResult fields */ },
		Language:                 "golang",
		RawBindings:              []string{"httpTrigger", "queueOutput"},
		FunctionId:               "b7a5c3f2-8d4e-4a7c-bc91-2f6e9d89e123",
		ManagedDependencyEnabled: true,
		RetryOptions:             &pb.RpcRetryOptions{ /* Initialize RpcRetryOptions fields */ },
		Properties: map[string]string{
			"timeout": "30s",
		},
	}

	return &metadata
}

// Convert rpc metadata to function info to store in registry
// Will be used when host sends us actual request to parse info
// and cast symbols for cx code
func getFunctionInfo(f interface{}, metadata *pb.RpcFunctionMetadata) *FunctionInfo {
	return &FunctionInfo{
		Func:            f,
		Name:            metadata.Name,
		Directory:       metadata.Directory,
		FunctionID:      metadata.FunctionId,
		HasReturn:       false,
		IsHTTPFunc:      false,
		InputTypes:      make(map[string]ParamTypeInfo),
		OutputTypes:     make(map[string]ParamTypeInfo),
		TriggerMetadata: make(map[string]interface{}),
	}
}

func (disp *Dispatcher) RegsiterHttpFunction(f interface{}) error {
	return nil
}

func (disp *Dispatcher) RegisterCosmosFunction(f interface{}) error {
	metadata := generateRPCMetadata()
	fi := getFunctionInfo(f, metadata)
	fr := disp.FunctionRegistry

	funcId := metadata.FunctionId
	if _, exists := fr.functions.Load(funcId); exists {
		return fmt.Errorf("function with ID %q already registered", funcId)
	}
	fr.functions.Store(funcId, fi)

	GetFunctionDetails(f)

	return nil
}
