module github.com/azure/azure-functions-golang-worker

go 1.24.0

require (
	github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2 v2.0.1
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.6.4
	google.golang.org/grpc v1.79.3
	google.golang.org/protobuf v1.36.10
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.20.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.11.2 // indirect
	github.com/Azure/go-amqp v1.4.0 // indirect
)

require (
	github.com/spf13/pflag v1.0.6
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/genproto v0.0.0-20230110181048-76db0878b65f // indirect
)
