package main

import (
	"github.com/azure/azure-functions-golang-worker/functions"
	functionrpc "github.com/azure/azure-functions-golang-worker/proto"
)

type MyStruct struct {
	string1 string
	string2 string
}

func hello(myStruct MyStruct) (string, MyStruct) {
	myString := "Hello"
	myStruct.string1 = myString
	myStruct.string2 = myString + "2"
	return myString, myStruct
}

func main() {
	// Temporary metadata call mock - host will give us the available metadata within the worker
	// Converted to function info to store in registry
	metadata := functionrpc.RpcFunctionMetadata{
		Name:       "MyFunction",
		Directory:  "/home/user/functions/my_function",
		ScriptFile: "handlergo",
		EntryPoint: "main",
		Bindings: map[string]*functionrpc.BindingInfo{
			"httpTrigger": { /* Initialize BindingInfo fields */ },
		},
		IsProxy:                  false,
		Status:                   &functionrpc.StatusResult{ /* Initialize StatusResult fields */ },
		Language:                 "golang",
		RawBindings:              []string{"httpTrigger", "queueOutput"},
		FunctionId:               "1234-5678-91011",
		ManagedDependencyEnabled: true,
		RetryOptions:             &functionrpc.RpcRetryOptions{ /* Initialize RpcRetryOptions fields */ },
		Properties: map[string]string{
			"timeout": "30s",
		},
	}

	// Create the app/handler
	app := functions.FunctionApp()

	// Register function(s)
	app.RegisterBlobFunction(hello, metadata.FunctionId, &metadata)

	// Start the worker
	app.StartWorkerServer()
}
