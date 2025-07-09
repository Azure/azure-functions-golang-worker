package functions

import (
	"fmt"
)

type RegisteredFunction struct {
	Func        interface{}
	FuncName    string
	FuncId      string
	RawBindings []Binding
	Retry       *RetryOptions
}

type FunctionDefinition struct {
	Func           interface{}
	FuncName       string
	FuncId         string
	InputBindings  map[string]GrpcBindingMetadata
	OutputBindings map[string]GrpcBindingMetadata
	Parameters     []Parameter
}

func (disp *Dispatcher) getFunction(funcId string) (*RegisteredFunction, error) {
	rf := disp.RegisteredFunctions
	funcVal, found := rf.Load(funcId)
	if !found {
		return nil, fmt.Errorf("function with ID %s not found", funcId)
	}

	res, casted := funcVal.(*RegisteredFunction)
	if !casted {
		return nil, fmt.Errorf("failed to cast RegisteredFunction for ID %s", funcId)
	}

	return res, nil
}

// func resolveTrigger(f interface{}, t Trigger) *FunctionInfo {
// 	switch tr := t.(type) {
// 	case *CosmosDBTrigger:
// 		return RegisterCosmosDBFunction(f, tr.ArgName, tr.ContainerName, tr.DatabaseName, tr.Connection)
// 	case *HttpTrigger:
// 		return RegisterHttpFunction(f, tr.Route)
// 	default:
// 		return nil
// 	}
// }

func (disp *Dispatcher) RegisterFunction(f interface{}, b Bind) *RegisteredFunction {
	binding := b.ToBinding()
	rf := RegisteredFunction{
		Func:        f,
		FuncName:    GetFunctionName(f),
		RawBindings: []Binding{binding},
	}

	funcId, err := HashFunctionID(rf)
	if err != nil {
		panic(err)
	}

	rf.FuncId = funcId
	disp.RegisteredFunctions.Store(funcId, rf)
	return &rf
}

func (rf *RegisteredFunction) WithRetry(retry *RetryOptions) *RegisteredFunction {
	rf.Retry = retry
	return rf
}
