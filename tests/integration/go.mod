module github.com/azure/azure-functions-golang-worker/tests/integration

go 1.25.7

require (
	github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos v1.3.0
	github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs v1.3.1
	github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus v1.9.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.7.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue v1.0.1
	github.com/azure/azure-functions-golang-worker/triggers/blob v0.0.0
	github.com/microsoft/go-mssqldb v1.10.0
)

replace github.com/azure/azure-functions-golang-worker => ../..

replace github.com/azure/azure-functions-golang-worker/triggers/blob => ../../triggers/blob

require (
	github.com/Azure/azure-sdk-for-go v68.0.0+incompatible // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.21.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.13.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.12.0 // indirect
	github.com/Azure/go-amqp v1.4.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.7.2 // indirect
	github.com/azure/azure-functions-golang-worker v0.7.0-preview // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	golang.org/x/crypto v0.51.0 // indirect
	golang.org/x/net v0.54.0 // indirect
	golang.org/x/sys v0.44.0 // indirect
	golang.org/x/text v0.37.0 // indirect
)
