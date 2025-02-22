package functions

import (
	"fmt"
	"log"
	"net/url"
	"reflect"
	"sync"

	pb "github.com/azure/azure-functions-golang-worker/proto"
	"github.com/spf13/pflag"
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
	FunctionsUri                  string
	FunctionsWorkerId             string
	FunctionsRequestId            string
	FunctionsGrpcMaxMessageLength int
}

func FunctionApp() *Dispatcher {
	args, err := getWorkerStartupConfig()
	if err != nil {
		log.Fatalf("Failed to parse command line arguments: %v", err)
	}

	return NewDispatcher(*args)
}

func getWorkerStartupConfig() (*WorkerStartupConfig, error) {
	args := parseArgs()
	err := validateArgs(args)

	return args, err
}

func parseArgs() *WorkerStartupConfig {
	// The host will send extra/older args that will be unused
	// e.g. --host, --port, --worker-id
	// The normal flag package will error out on these and cannot be changed
	pflag.CommandLine.ParseErrorsWhitelist.UnknownFlags = true

	functionsURI := pflag.String("functions-uri", "", "URI with IP Address and Port used to connect to the Host via gRPC.")
	functionsWorkerID := pflag.String("functions-worker-id", "", "Worker ID assigned to this language worker.")
	functionsRequestID := pflag.String("functions-request-id", "", "Request ID used for gRPC communication with the Host.")
	functionsGrpcMaxMsgLen := pflag.Int("functions-grpc-max-message-length", DefaultFunctionsGrpcMaxMsgLen, "Max grpc message length for Functions")
	pflag.Parse()

	return &WorkerStartupConfig{
		FunctionsUri:                  *functionsURI,
		FunctionsWorkerId:             *functionsWorkerID,
		FunctionsRequestId:            *functionsRequestID,
		FunctionsGrpcMaxMessageLength: *functionsGrpcMaxMsgLen,
	}
}

func validateArgs(args *WorkerStartupConfig) error {
	if args.FunctionsUri == "" {
		return fmt.Errorf("missing required argument: --functions-uri")
	}
	if _, err := url.Parse(args.FunctionsUri); err != nil {
		return fmt.Errorf("invalid --functions-uri provided (%s): %v", args.FunctionsUri, err)
	}
	if args.FunctionsWorkerId == "" {
		return fmt.Errorf("missing required argument: --functions-worker-id")
	}
	if args.FunctionsRequestId == "" {
		return fmt.Errorf("missing required argument: --functions-request-id")
	}

	return nil
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

// RegisterCosmosFunction stores the given metadata under the specified function ID for Cosmos only
// To have more control over inputs and outputs, we can have type specific functions that the cx can use
func (disp *Dispatcher) RegisterCosmosFunction(f interface{}) error {
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

	GetFunctionDetails(f)

	return nil
}
