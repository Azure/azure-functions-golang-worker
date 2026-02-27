module myapp

go 1.25.5

require github.com/azure/azure-functions-golang-worker v0.0.0

require (
	github.com/spf13/pflag v1.0.6 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
	golang.org/x/text v0.31.0 // indirect
	google.golang.org/genproto v0.0.0-20230110181048-76db0878b65f // indirect
	google.golang.org/grpc v1.70.0 // indirect
	google.golang.org/protobuf v1.36.4 // indirect
)

replace github.com/azure/azure-functions-golang-worker => ../..
