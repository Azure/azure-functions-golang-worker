package bindings

// TimerTrigger is the binding type constant for timer triggers.
const TimerTriggerType BindingType = "timerTrigger"

// TimerBinding is the JSON wire format for timer trigger bindings,
// embedded in Binding and flattened during serialization.
type TimerBinding struct {
	Schedule string `json:"schedule"`
}

// TimerInfo represents the runtime payload passed to a timer-triggered function
// by the Azure Functions host. It contains schedule metadata and past-due status.
type TimerInfo struct {
	Schedule       TimerSchedule       `json:"schedule"`
	ScheduleStatus TimerScheduleStatus `json:"scheduleStatus"`
	IsPastDue      bool                `json:"isPastDue"`
}

// TimerSchedule contains schedule configuration metadata.
type TimerSchedule struct {
	AdjustForDST bool `json:"adjustForDST"`
}

// TimerScheduleStatus contains the last and next execution times
// as reported by the Azure Functions host.
type TimerScheduleStatus struct {
	Last        string `json:"last"`
	Next        string `json:"next"`
	LastUpdated string `json:"lastUpdated"`
}

// TimerTrigger is the user-facing configuration for registering a timer trigger.
// Use App.Timer() to create one via the fluent builder API.
type TimerTrigger struct {
	Name     string
	Schedule string
}

// GetBindingType returns the timer trigger binding type.
func (t *TimerTrigger) GetBindingType() BindingType { return TimerTriggerType }

// ToBinding converts the user-facing TimerTrigger into an internal Binding
// suitable for serialization to the Azure Functions host.
func (t *TimerTrigger) ToBinding() Binding {
	return Binding{
		Name:         t.Name,
		Type:         string(t.GetBindingType()),
		Direction:    "in",
		TimerBinding: &TimerBinding{Schedule: t.Schedule},
	}
}
