module github.com/azure/azure-functions-golang-worker/tests/integration/testdata/durableFunctions

go 1.25.0

replace github.com/azure/azure-functions-golang-worker => ../../../..

replace github.com/azure/azure-functions-golang-worker/middleware/durabletask => ../../../../middleware/durabletask

replace google.golang.org/genproto => google.golang.org/genproto v0.0.0-20250528174236-200df99c418a

require (
	github.com/azure/azure-functions-golang-worker v0.6.0-preview
	github.com/azure/azure-functions-golang-worker/middleware/durabletask v0.0.0-00010101000000-000000000000
	github.com/microsoft/durabletask-go v0.6.0
)

require (
	github.com/cenkalti/backoff/v4 v4.1.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/marusama/semaphore/v2 v2.5.0 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.43.0 // indirect
	go.opentelemetry.io/otel/metric v1.43.0 // indirect
	go.opentelemetry.io/otel/trace v1.43.0 // indirect
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/grpc v1.81.1 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)
