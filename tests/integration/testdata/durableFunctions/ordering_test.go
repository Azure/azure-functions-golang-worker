package main

import "testing"

func TestCompletedInvocations_ExcludesInFlightObservers(t *testing.T) {
	inFlight := &invocationProbe{
		Name: "Blocked", Error: "blocked by ordering probe",
		Steps: []string{"A:before", "B:before", "B:after"},
	}
	completed := &invocationProbe{Name: "InputProbe", Complete: true}
	records := map[string]*invocationProbe{"in-flight": inFlight, "complete": completed}
	got := completedInvocations(records)
	if len(got) != 1 || got["complete"] != completed {
		t.Fatalf("snapshot published an incomplete invocation: %+v", got)
	}
	inFlight.Steps = append(inFlight.Steps, "A:after")
	if _, ok := completedInvocations(records)["in-flight"]; ok {
		t.Fatal("observer completion alone must not precede outer OTel completion")
	}
	inFlight.Complete = true
	if got := completedInvocations(records); len(got) != 2 || got["in-flight"] != inFlight {
		t.Fatalf("completed invocation is missing: %+v", got)
	}
}
