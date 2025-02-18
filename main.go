package main

import (
	"github.com/azure/azure-functions-golang-worker/function"
	"github.com/azure/azure-functions-golang-worker/internal"
	functionrpc "github.com/azure/azure-functions-golang-worker/proto"
)

func hello() (string, error) {
	return "Hello λ!", nil
}

func main() {
	// TODO: likely to be combined with register function - one time setup
	// Consider how host will start the Go worker
	function.SetupWorker()

	// Temporary metadata call mock - host will give us the available metadata within the worker
	// Converted to function info to store in registry
	metadata := functionrpc.RpcFunctionMetadata{
		Name:       "MyFunction",
		Directory:  "/home/user/functions/my_function",
		ScriptFile: "handler.py",
		EntryPoint: "main",
		Bindings: map[string]*functionrpc.BindingInfo{
			"httpTrigger": { /* Initialize BindingInfo fields */ },
		},
		IsProxy:                  false,
		Status:                   &functionrpc.StatusResult{ /* Initialize StatusResult fields */ },
		Language:                 "python",
		RawBindings:              []string{"httpTrigger", "queueOutput"},
		FunctionId:               "1234-5678-91011",
		ManagedDependencyEnabled: true,
		RetryOptions:             &functionrpc.RpcRetryOptions{ /* Initialize RpcRetryOptions fields */ },
		Properties: map[string]string{
			"timeout": "30s",
		},
	}

	internal.RegisterBlobFunction(hello, metadata.FunctionId, &metadata)
}
