package functions

import (
	"flag"
	"fmt"
	"log"
	"net/url"
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

type FunctionRegistry struct {
	mu        sync.RWMutex
	functions map[string]*FunctionInfo
}

type WorkerStartupConfig struct {
	HostAddress                   string
	HostRequestId                 string
	WorkerId                      string
	FunctionsGrpcMaxMessageLength int
}

func FunctionApp() *Dispatcher {
	// TODO: use and enforce these args in dispatcher
	args, err := getCmdLineArgs()
	if err != nil {
		log.Fatalf("Failed to parse command line arguments: %v", err)
	}

	return NewDispatcher(*args)
}

func getCmdLineArgs() (*WorkerStartupConfig, error) {
	functionsURI := flag.String("functions-uri", "", "The host's gRPC endpoint URI (e.g. http://127.0.0.1:12345)")
	requestID := flag.String("functions-request-id", "", "The request ID passed by the host")
	workerID := flag.String("functions-worker-id", "", "The worker ID passed by the host")
	grpcMaxMessageLength := flag.Int("functions-grpc-max-message-length", 4*1024*1024, "Max gRPC message length")

	flag.Parse()

	if *functionsURI == "" {
		return nil, fmt.Errorf("missing required argument: --functions-uri")
	}

	parsedURI, err := url.Parse(*functionsURI)
	if err != nil {
		return nil, fmt.Errorf("invalid --functions-uri provided (%s): %v", *functionsURI, err)
	}
	if *requestID == "" {
		return nil, fmt.Errorf("missing required argument: --functions-request-id")
	}
	if *workerID == "" {
		return nil, fmt.Errorf("missing required argument: --functions-worker-id")
	}

	address := parsedURI.Host
	if address == "" {
		address = DefaultHostPort
	}

	return &WorkerStartupConfig{
		HostAddress:                   *functionsURI,
		HostRequestId:                 *requestID,
		WorkerId:                      *workerID,
		FunctionsGrpcMaxMessageLength: *grpcMaxMessageLength,
	}, nil
}

func generateRPCMetadata() *functionrpc.RpcFunctionMetadata {
	metadata := functionrpc.RpcFunctionMetadata{
		Name:       "MyFunction",
		Directory:  "/home/user/functions/my_function",
		ScriptFile: "handler.go",
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

	return &metadata
}

// Convert rpc metadata to function info to store in registry
// Will be used when host sends us actual request to parse info
// and cast symbols for cx code
func getFunctionInfo(f interface{}, metadata *functionrpc.RpcFunctionMetadata) *FunctionInfo {
	// Convert metadata to function info
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

// RegisterBlobFunction stores the given metadata under the specified function ID for Blobs only
// To have more control over inputs and outputs, we can have type specific functions that the cx can use
// Temporarily passing the function and metadata separate - extract for translation
func (disp Dispatcher) RegisterBlobFunction(f interface{}) error {
	metadata := generateRPCMetadata()
	fi := getFunctionInfo(f, metadata)
	fr := disp.FunctionRegistry
	fr.mu.Lock()
	defer fr.mu.Unlock()

	funcId := metadata.FunctionId
	if _, exists := fr.functions[funcId]; exists {
		return fmt.Errorf("function with ID %q already registered", funcId)
	}
	fr.functions[funcId] = fi

	return nil
}
