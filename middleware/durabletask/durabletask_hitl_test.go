package durabletask

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/task"
)

// approvalDecision mirrors the sample's HITL event payload.
type approvalDecision struct {
	Approved bool `json:"approved"`
}

// startEmulatorHub stands up a full in-memory durabletask worker (the
// "emulator") that actually executes orchestrators and activities, plus a
// client. Unlike newEmulatorBackend (which is used to capture a single
// OrchestratorRequest for the replay-equivalence tests), this runs the whole
// task hub so multi-turn orchestrations — fan-out, timers, external events —
// drive to completion.
func startEmulatorHub(t *testing.T, r *task.TaskRegistry) backend.TaskHubClient {
	t.Helper()
	ctx := context.Background()
	logger := backend.DefaultLogger()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	executor := task.NewTaskExecutor(r)
	orchestrationWorker := backend.NewOrchestrationWorker(be, executor, logger)
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
	hub := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger)
	if err := hub.Start(ctx); err != nil {
		t.Fatalf("start hub: %v", err)
	}
	t.Cleanup(func() { _ = hub.Shutdown(ctx) })
	return backend.NewTaskHubClient(be)
}

// TestEndToEnd_FanOutAndExternalEvent validates the core patterns the
// durableFunctions sample's ProcessExpense orchestration relies on, executed
// end-to-end on the emulator: fan-out/fan-in across parallel activities,
// custom-status progress, and a human-in-the-loop approval delivered as an
// external event (the HITL response). The approved path must complete with the
// "approved" output.
func TestEndToEnd_FanOutAndExternalEvent(t *testing.T) {
	ctx := context.Background()

	r := task.NewTaskRegistry()
	if err := r.AddOrchestratorN("Approval", func(octx *task.OrchestrationContext) (any, error) {
		// Fan-out: schedule both checks before awaiting either.
		t1 := octx.CallActivity("Check", task.WithActivityInput("a"))
		t2 := octx.CallActivity("Check", task.WithActivityInput("b"))
		var r1, r2 bool
		if err := t1.Await(&r1); err != nil {
			return nil, err
		}
		if err := t2.Await(&r2); err != nil {
			return nil, err
		}

		octx.SetCustomStatus("awaiting approval")

		// HITL: wait for the external approval event (with a timeout).
		var d approvalDecision
		if err := octx.WaitForSingleEvent("ApprovalDecision", 30*time.Second).Await(&d); err != nil {
			return "timeout", nil
		}
		if d.Approved && r1 && r2 {
			return "approved", nil
		}
		return "rejected", nil
	}); err != nil {
		t.Fatalf("add orchestrator: %v", err)
	}
	if err := r.AddActivityN("Check", func(task.ActivityContext) (any, error) {
		return true, nil
	}); err != nil {
		t.Fatalf("add activity: %v", err)
	}

	client := startEmulatorHub(t, r)

	id, err := client.ScheduleNewOrchestration(ctx, "Approval")
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}

	// Wait until the instance is running, then deliver the HITL approval.
	// durabletask buffers the event, so even if it arrives before the
	// orchestrator reaches WaitForSingleEvent it is consumed on the next turn.
	if _, err := client.WaitForOrchestrationStart(ctx, id); err != nil {
		t.Fatalf("wait start: %v", err)
	}
	if err := client.RaiseEvent(ctx, id, "ApprovalDecision",
		api.WithEventPayload(approvalDecision{Approved: true})); err != nil {
		t.Fatalf("raise event: %v", err)
	}

	metadata, err := client.WaitForOrchestrationCompletion(ctx, id)
	if err != nil {
		t.Fatalf("wait completion: %v", err)
	}
	if metadata.RuntimeStatus != api.RUNTIME_STATUS_COMPLETED {
		t.Fatalf("status = %v, want COMPLETED", metadata.RuntimeStatus)
	}
	if !strings.Contains(metadata.SerializedOutput, "approved") {
		t.Fatalf("output = %q, want approved", metadata.SerializedOutput)
	}
}
