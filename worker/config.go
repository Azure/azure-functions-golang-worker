package worker

import (
	"fmt"
	"net/url"

	"github.com/spf13/pflag"
)

type WorkerStartupConfig struct {
	FunctionsUri                  string
	FunctionsWorkerId             string
	FunctionsRequestId            string
	FunctionsGrpcMaxMessageLength int
}

const DefaultFunctionsGrpcMaxMsgLen = 134217728

func GetWorkerStartupConfig() (*WorkerStartupConfig, error) {
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

	args.FunctionsUri = CleanAddressForGrpc(args.FunctionsUri)

	if args.FunctionsWorkerId == "" {
		return fmt.Errorf("missing required argument: --functions-worker-id")
	}
	if args.FunctionsRequestId == "" {
		return fmt.Errorf("missing required argument: --functions-request-id")
	}

	return nil
}

func CleanAddressForGrpc(address string) string {
	u, err := url.Parse(address)
	if err == nil && (u.Scheme == "http" || u.Scheme == "https") {
		return u.Host
	}
	return address
}
