package bindings

import (
	"encoding/json"
	"testing"
)

func TestTimerTriggerGetBindingType(t *testing.T) {
	trigger := &TimerTrigger{
		Name:     "myTimer",
		Schedule: "0 */5 * * * *",
	}

	if got := trigger.GetBindingType(); got != TimerTriggerBindingType {
		t.Errorf("GetBindingType() = %q, want %q", got, TimerTriggerBindingType)
	}
}

func TestTimerTriggerToBinding(t *testing.T) {
	trigger := &TimerTrigger{
		Name:     "myTimer",
		Schedule: "0 */5 * * * *",
	}

	b := trigger.ToBinding()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"Name", b.Name, "myTimer"},
		{"Type", b.Type, "timerTrigger"},
		{"Direction", b.Direction, "in"},
	}
	for _, tc := range tests {
		if tc.got != tc.want {
			t.Errorf("Binding.%s = %q, want %q", tc.name, tc.got, tc.want)
		}
	}
	if b.TimerBinding == nil {
		t.Fatal("Binding.TimerBinding is nil")
	}
	if b.TimerBinding.Schedule != "0 */5 * * * *" {
		t.Errorf("TimerBinding.Schedule = %q, want %q", b.TimerBinding.Schedule, "0 */5 * * * *")
	}
}

func TestTimerTriggerToBindingJSON(t *testing.T) {
	trigger := &TimerTrigger{
		Name:     "myTimer",
		Schedule: "0 */5 * * * *",
	}

	data, err := json.Marshal(trigger.ToBinding())
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	// Verify that binding-specific fields are flattened into the top-level JSON object
	want := map[string]string{
		"name":      "myTimer",
		"type":      "timerTrigger",
		"direction": "in",
		"schedule":  "0 */5 * * * *",
	}
	for key, wantVal := range want {
		got, ok := m[key]
		if !ok {
			t.Errorf("JSON missing key %q", key)
			continue
		}
		if got != wantVal {
			t.Errorf("JSON[%q] = %v, want %q", key, got, wantVal)
		}
	}
}

func TestTimerInfoDeserialization(t *testing.T) {
	input := `{
		"schedule": {
			"adjustForDST": true
		},
		"scheduleStatus": {
			"last": "2026-03-05T10:00:00+00:00",
			"next": "2026-03-05T10:05:00+00:00",
			"lastUpdated": "2026-03-05T10:00:01+00:00"
		},
		"isPastDue": false
	}`

	var info TimerInfo
	if err := json.Unmarshal([]byte(input), &info); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if !info.Schedule.AdjustForDST {
		t.Error("Schedule.AdjustForDST = false, want true")
	}
	if info.ScheduleStatus.Last != "2026-03-05T10:00:00+00:00" {
		t.Errorf("ScheduleStatus.Last = %q, want %q", info.ScheduleStatus.Last, "2026-03-05T10:00:00+00:00")
	}
	if info.ScheduleStatus.Next != "2026-03-05T10:05:00+00:00" {
		t.Errorf("ScheduleStatus.Next = %q, want %q", info.ScheduleStatus.Next, "2026-03-05T10:05:00+00:00")
	}
	if info.ScheduleStatus.LastUpdated != "2026-03-05T10:00:01+00:00" {
		t.Errorf("ScheduleStatus.LastUpdated = %q, want %q", info.ScheduleStatus.LastUpdated, "2026-03-05T10:00:01+00:00")
	}
	if info.IsPastDue {
		t.Error("IsPastDue = true, want false")
	}
}

func TestTimerInfoIsPastDue(t *testing.T) {
	cases := []struct {
		name     string
		json     string
		wantPast bool
	}{
		{
			name:     "isPastDue true",
			json:     `{"schedule":{"adjustForDST":false},"scheduleStatus":{"last":"","next":"","lastUpdated":""},"isPastDue":true}`,
			wantPast: true,
		},
		{
			name:     "isPastDue false",
			json:     `{"schedule":{"adjustForDST":false},"scheduleStatus":{"last":"","next":"","lastUpdated":""},"isPastDue":false}`,
			wantPast: false,
		},
		{
			name:     "isPastDue missing defaults to false",
			json:     `{"schedule":{"adjustForDST":false},"scheduleStatus":{"last":"","next":"","lastUpdated":""}}`,
			wantPast: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var info TimerInfo
			if err := json.Unmarshal([]byte(tc.json), &info); err != nil {
				t.Fatalf("json.Unmarshal failed: %v", err)
			}
			if info.IsPastDue != tc.wantPast {
				t.Errorf("IsPastDue = %v, want %v", info.IsPastDue, tc.wantPast)
			}
		})
	}
}
