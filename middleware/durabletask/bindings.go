package durabletask

import (
	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
)

// Trigger / binding type constants the host's DurableTask extension
// recognizes. They match the binding "type" strings the in-process and
// other out-of-process workers emit.
const (
	// OrchestrationTriggerType identifies an orchestrator function. Its
	// invocations carry a base64-encoded OrchestratorRequest and are
	// intercepted by the durable middleware for replay.
	OrchestrationTriggerType bindings.BindingType = "orchestrationTrigger"

	// ActivityTriggerType identifies an activity function. Activities run
	// through the normal worker pipeline (input in, result out).
	ActivityTriggerType bindings.BindingType = "activityTrigger"

	// DurableClientBindingType identifies a durable client input binding. A
	// starter function declares it (via [ClientInput]) to receive the host's
	// durable gRPC endpoint, which the middleware uses to reach the management
	// client. It matches the binding "type" string the other language workers
	// emit for a durable client binding.
	DurableClientBindingType bindings.BindingType = "durableClient"
)

// Binding parameter names. These are the binding "name" values the host
// echoes back as the InputData parameter name, and which the worker maps to
// the function's trigger argument.
const (
	orchestrationParamName = "context"
	activityParamName      = "input"

	// durableClientParamName is the binding name for a durable client input
	// binding. The host echoes it as the InputData entry name carrying the
	// durable gRPC endpoint JSON, which the middleware looks up via
	// [sdk.MiddlewareContext.BindingInput].
	durableClientParamName = "durableClient"
)

// orchestrationTriggerBinding is the [bindings.Bind] implementation for
// orchestrator functions. The base name/type/direction fields are all the
// host needs to recognize the trigger; no extra sub-binding payload is
// required because the orchestration name defaults to the function name.
type orchestrationTriggerBinding struct{}

func (orchestrationTriggerBinding) GetBindingType() bindings.BindingType {
	return OrchestrationTriggerType
}

func (orchestrationTriggerBinding) ToBinding() bindings.Binding {
	return bindings.Binding{
		Name:      orchestrationParamName,
		Type:      string(OrchestrationTriggerType),
		Direction: "in",
	}
}

// activityTriggerBinding is the [bindings.Bind] implementation for activity
// functions.
type activityTriggerBinding struct{}

func (activityTriggerBinding) GetBindingType() bindings.BindingType {
	return ActivityTriggerType
}

func (activityTriggerBinding) ToBinding() bindings.Binding {
	return bindings.Binding{
		Name:      activityParamName,
		Type:      string(ActivityTriggerType),
		Direction: "in",
	}
}

// durableClientBinding is the [bindings.Bind] implementation for a durable
// client input binding. The base name/type/direction fields are all the host
// needs to deliver the durable gRPC endpoint as the binding's input value.
type durableClientBinding struct{}

func (durableClientBinding) GetBindingType() bindings.BindingType {
	return DurableClientBindingType
}

func (durableClientBinding) ToBinding() bindings.Binding {
	return bindings.Binding{
		Name:      durableClientParamName,
		Type:      string(DurableClientBindingType),
		Direction: "in",
	}
}

// ClientInput declares that a starter function needs a durable management
// [Client]. It adds a durableClient input binding so the Functions host
// delivers the durable gRPC endpoint with each invocation; the middleware
// connects to it (once per endpoint) and attaches the client to the context,
// where the function retrieves it via [ClientFromContext]:
//
//	app.HTTP("start", StartHelloCities,
//	    sdk.WithMethods("post"), durabletask.ClientInput())
//
// The binding is appended after the function's trigger binding, so it never
// displaces the trigger argument. Without it, the middleware falls back to the
// [EnvGrpcEndpoint] client (if configured), which is mainly useful for tests
// and standalone scenarios.
func ClientInput() sdk.Option {
	return func(rf *sdk.RegisteredFunction) {
		rf.RawBindings = append(rf.RawBindings, durableClientBinding{}.ToBinding())
	}
}
