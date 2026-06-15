package durabletask

import "github.com/azure/azure-functions-golang-worker/sdk/bindings"

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
)

// Binding parameter names. These are the binding "name" values the host
// echoes back as the InputData parameter name, and which the worker maps to
// the function's trigger argument.
const (
	orchestrationParamName = "context"
	activityParamName      = "input"
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
