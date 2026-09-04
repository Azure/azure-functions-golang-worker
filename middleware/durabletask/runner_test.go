package durabletask

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/task"
)

// A first turn carries ExecutionStarted in newEvents and no past events; every
// later turn carries past events. Those are the only shapes the host produces,
// verified against captured requests from sequential, fan-out, timer,
// external-event, ContinueAsNew and sub-orchestration runs.
func TestCheckHistoryPresent_AcceptsFirstAndLaterTurns(t *testing.T) {
	be, client := newEmulatorBackend(t)
	encoded := scheduleAndEncodeRequest(t, be, client, "HelloCities", "")

	instanceID, pastEvents, newEvents := decodeRequest(t, encoded)

	if len(pastEvents) != 0 {
		t.Fatalf("expected a first turn with no past events, got %d", len(pastEvents))
	}
	if err := checkHistoryPresent(instanceID, pastEvents, newEvents); err != nil {
		t.Errorf("first turn rejected: %v", err)
	}

	// A later turn has past events and no ExecutionStarted among the new ones.
	if err := checkHistoryPresent(instanceID, newEvents, nil); err != nil {
		t.Errorf("later turn rejected: %v", err)
	}
}

// Empty past events with no ExecutionStarted means the host withheld the
// history, which only happens under extended sessions. Replaying that would
// re-schedule completed work, so it must fail rather than proceed.
func TestCheckHistoryPresent_RejectsWithheldHistory(t *testing.T) {
	be, client := newEmulatorBackend(t)
	encoded := scheduleAndEncodeRequest(t, be, client, "HelloCities", "")

	_, _, newEvents := decodeRequest(t, encoded)

	// Drop the ExecutionStarted event to simulate a host that withheld history.
	withoutStart := make([]*backend.HistoryEvent, 0, len(newEvents))
	for _, event := range newEvents {
		if event.GetExecutionStarted() == nil {
			withoutStart = append(withoutStart, event)
		}
	}
	if len(withoutStart) == len(newEvents) {
		t.Fatal("expected the first turn to contain an ExecutionStarted event")
	}

	err := checkHistoryPresent("id-1", nil, withoutStart)
	if err == nil {
		t.Fatal("expected an error when the history was withheld")
	}
	if !strings.Contains(err.Error(), "extended sessions") {
		t.Errorf("expected the error to name the cause, got %q", err)
	}
}

// durabletask-go's own backend fills in a missing sub-orchestration instance
// ID. The Functions host does not: it forwards InstanceId="" and the resulting
// message is never delivered, so the child never starts and the parent waits
// forever with no error. The worker fills it instead, using the same
// deterministic format the durabletask-go backend uses so an orchestrator
// produces identical child instance IDs either way.
func TestAssignSubOrchestrationIDs(t *testing.T) {
	tests := []struct {
		name         string
		orchestrator task.Orchestrator
		want         string
	}{
		{
			name: "generated when the caller supplies none",
			orchestrator: func(ctx *task.OrchestrationContext) (any, error) {
				var out []string
				return nil, ctx.CallSubOrchestrator("HelloCities").Await(&out)
			},
			// "<parent>:<action sequence number in hex>", the format
			// durabletask-go's backend uses when it fills the gap itself.
			want: "parent-1:0000",
		},
		{
			name: "caller-supplied ID is preserved",
			orchestrator: func(ctx *task.OrchestrationContext) (any, error) {
				var out []string
				return nil, ctx.CallSubOrchestrator("HelloCities",
					task.WithSubOrchestrationInstanceID("chosen-by-caller")).Await(&out)
			},
			want: "chosen-by-caller",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := task.NewTaskRegistry()
			if err := registry.AddOrchestratorN("Parent", test.orchestrator); err != nil {
				t.Fatalf("register parent: %v", err)
			}
			if err := registry.AddOrchestratorN("HelloCities", helloCities); err != nil {
				t.Fatalf("register child: %v", err)
			}

			results := executeFirstTurn(t, registry, "parent-1", "Parent")
			assignSubOrchestrationIDs("parent-1", results)

			var got string
			for _, action := range results.Response.GetActions() {
				if createSO := action.GetCreateSubOrchestration(); createSO != nil {
					got = createSO.GetInstanceId()
				}
			}
			if got != test.want {
				t.Errorf("sub-orchestration instance ID = %q, want %q", got, test.want)
			}
		})
	}
}

// Actions other than CreateSubOrchestration are left untouched.
func TestAssignSubOrchestrationIDs_IgnoresOtherActions(t *testing.T) {
	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("HelloCities", helloCities); err != nil {
		t.Fatalf("register orchestrator: %v", err)
	}

	results := executeFirstTurn(t, registry, "id-1", "HelloCities")
	before := len(results.Response.GetActions())

	assignSubOrchestrationIDs("id-1", results)

	if got := len(results.Response.GetActions()); got != before {
		t.Errorf("action count changed from %d to %d", before, got)
	}
	for _, action := range results.Response.GetActions() {
		if action.GetCreateSubOrchestration() != nil {
			t.Error("did not expect a sub-orchestration action")
		}
	}
}

func TestAssignSubOrchestrationIDs_ToleratesNilResults(t *testing.T) {
	assignSubOrchestrationIDs("id-1", nil)
	assignSubOrchestrationIDs("id-1", &backend.ExecutionResults{})
}

// executeFirstTurn runs the named orchestrator's first turn and returns the
// engine's results, which carry the actions the orchestrator scheduled.
func executeFirstTurn(t *testing.T, registry *task.TaskRegistry, instanceID, name string) *backend.ExecutionResults {
	t.Helper()

	be, client := newEmulatorBackend(t)
	encoded := scheduleAndEncodeRequest(t, be, client, name, "")
	_, pastEvents, newEvents := decodeRequest(t, encoded)

	executor := task.NewTaskExecutor(registry)
	results, err := executor.ExecuteOrchestrator(context.Background(), api.InstanceID(instanceID), pastEvents, newEvents)
	if err != nil {
		t.Fatalf("execute orchestrator: %v", err)
	}
	return results
}

// decodeRequest unwraps a base64-encoded OrchestratorRequest into the pieces
// the runner works with.
func decodeRequest(t *testing.T, encoded string) (string, []*backend.HistoryEvent, []*backend.HistoryEvent) {
	t.Helper()

	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode base64 request: %v", err)
	}
	instanceID, pastEvents, newEvents, err := decodeOrchestratorRequest(raw)
	if err != nil {
		t.Fatalf("parse orchestrator request: %v", err)
	}
	return instanceID, pastEvents, newEvents
}
