package functions

import (
	"encoding/json"
	"fmt"
	"reflect"

	pb "github.com/azure/azure-functions-golang-worker/proto"
)

type BindingType string

const (
	CosmosDBFunction BindingType = "cosmosDBTrigger"
	HttpFunction     BindingType = "httpTrigger"
)

type BindingDirection int

const (
	In BindingDirection = iota
	Out
)

// type BindingDataType string

// const (
// 	String BindingDataType = "string"
// 	Binary BindingDataType = "binary"
// 	Stream BindingDataType = "stream"
// )

type Bind interface {
	GetBindingType() BindingType
	ToBinding() Binding
}

type Binding struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Direction string `json:"direction"`

	*CosmosDBBinding
	*HTTPBinding
}

type GrpcBindingMetadata struct {
	Name      string
	Type      string
	Direction BindingDirection
}

type Parameter struct {
	Name     string
	DataType reflect.Type
}

func BuildRpcRawBindings(rawBindings []Binding) []string {
	var rpcRawBindings []string
	for _, rb := range rawBindings {
		rbJson, err := json.Marshal(rb)
		if err != nil {
			panic(fmt.Sprintf("Error marshalling binding for %v: %v", rb.Type, err))
		}
		rpcRawBindings = append(rpcRawBindings, string(rbJson))
	}
	return rpcRawBindings
}

func GetBindingInfoList(rawBindings []Binding) map[string]*pb.BindingInfo {
	bindings := make(map[string]*pb.BindingInfo)
	for _, rb := range rawBindings {
		funcDirStr := rb.Direction
		funcTypeStr := rb.Type
		funcDirInt, ok := pb.BindingInfo_Direction_value[funcDirStr]
		if !ok {
			panic("Bindings must declare a direction and type: " + funcDirStr)
		}

		// TODO: Check for DataType here (not in the current functions we support like blob)
		jsonName := rb.Name
		bindings[jsonName] = &pb.BindingInfo{
			Direction: pb.BindingInfo_Direction(funcDirInt),
			Type:      funcTypeStr,
		}
	}
	return bindings
}
