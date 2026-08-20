module github.com/azure/azure-functions-golang-worker/tests/integration/testdata/blobTrigger

go 1.25.7

require (
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.7.0
	github.com/azure/azure-functions-golang-worker v0.6.0-preview
	github.com/azure/azure-functions-golang-worker/triggers/blob v0.0.0
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.21.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.13.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.12.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.7.2 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	golang.org/x/crypto v0.51.0 // indirect
	golang.org/x/net v0.54.0 // indirect
	golang.org/x/sys v0.44.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260120221211-b8f7ae30c516 // indirect
	google.golang.org/grpc v1.80.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/azure/azure-functions-golang-worker => ../../../..

replace github.com/azure/azure-functions-golang-worker/triggers/blob => ../../../../triggers/blob
