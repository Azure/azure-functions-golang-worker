package functions

import "reflect"

type HttpTrigger struct {
	Route string
}

type HttpRequest struct {
	Headers     map[string]string
	Method      string
	Url         string
	QueryParams map[string]string
	RouteParams map[string]string
}

// type HttpResponse struct {
// 	StatusCode int
// 	Body       string
// 	Headers    map[string]string
// }

func (c *HttpTrigger) GetTriggerType() TriggerType { return Http }

func RegisterHttpFunction(f interface{}, route string, authLevel AuthorizationLevel) *FunctionInfo {
	inputTypes := make(map[string]ParamTypeInfo)
	inputTypes[route] = ParamTypeInfo{
		BindingName: "httpTrigger",
		ParamType:   reflect.TypeOf(HttpRequest{}),
	}

	triggerMetadata := make(map[string]string)
	triggerMetadata["name"] = "req"
	triggerMetadata["direction"] = "IN"
	triggerMetadata["type"] = "httpTrigger"
	triggerMetadata["route"] = route
	triggerMetadata["authLevel"] = "ANONYMOUS"
	triggerMetadata["param_name"] = "req"

	return &FunctionInfo{
		Func:            f,
		Name:            "HttpTrigger",
		Directory:       "Dir",
		FunctionID:      "0f7b4505-98b8-4bd2-b71a-3ec427bd4c58",
		HasReturn:       false,
		IsHTTPFunc:      true,
		InputTypes:      inputTypes,
		OutputTypes:     make(map[string]ParamTypeInfo),
		TriggerMetadata: triggerMetadata,
	}
}
