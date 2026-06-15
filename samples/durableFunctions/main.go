// Command durableFunctions is a sample Azure Functions app showing Durable
// Functions in the Go worker via the durabletask integration (model 2:
// gRPC work-item stream).
//
// It demonstrates, in a single app:
//
//   - Multiple orchestrations registered side by side (HelloCities and
//     ProcessExpense).
//   - The three building blocks: orchestrators (deterministic coordinators),
//     activities (the actual work, durabletask-go native signature), and HTTP
//     starters/management endpoints.
//   - Fan-out / fan-in, custom-status progress reporting, and a
//     human-in-the-loop (HITL) approval gated by a durable timeout.
//   - How orchestration endpoints are exposed: start, status/progress, and
//     raising an external event (the HITL response).
//
// The durable feature is wired with two registrations: app.Use(dt.Middleware())
// injects the durable client into starter functions, and
// worker.Start(app, sdk.WithLifecycleHook(dt)) runs the work-item listener
// that executes orchestrators and activities.
package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/azure/azure-functions-golang-worker/middleware/durabletask"
	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker"
	"github.com/microsoft/durabletask-go/task"
)

const (
	// autoApproveLimit is the amount (inclusive) below which expenses are
	// approved automatically; above it a human must approve.
	autoApproveLimit = 100.0

	// approvalTimeout is how long ProcessExpense waits for a human decision
	// before auto-rejecting. A durable timer backs this, so it survives
	// worker restarts.
	approvalTimeout = 72 * time.Hour
)

// =============================================================================
// Orchestration 1: HelloCities — a simple sequential workflow.
// =============================================================================

// HelloCities calls the SayHello activity for each city in sequence and
// returns the collected greetings.
//
// Orchestrator code is replayed by the framework, so it must be
// deterministic: do all I/O and non-determinism inside activities.
func HelloCities(ctx *task.OrchestrationContext) (any, error) {
	cities := []string{"Tokyo", "Seattle", "London"}
	greetings := make([]string, 0, len(cities))
	for _, city := range cities {
		var greeting string
		if err := ctx.CallActivity("SayHello", task.WithActivityInput(city)).Await(&greeting); err != nil {
			return nil, err
		}
		greetings = append(greetings, greeting)
	}
	return greetings, nil
}

// SayHello is an activity. Activities use durabletask-go's native signature
// (func(task.ActivityContext) (any, error)); they read input with
// ctx.GetInput and run once (no replay), so non-deterministic code is fine.
func SayHello(ctx task.ActivityContext) (any, error) {
	var city string
	if err := ctx.GetInput(&city); err != nil {
		return nil, err
	}
	return fmt.Sprintf("Hello, %s!", city), nil
}

// =============================================================================
// Orchestration 2: ProcessExpense — fan-out/fan-in + HITL approval.
// =============================================================================

// Expense is the orchestration input.
type Expense struct {
	ID         string  `json:"id"`
	Submitter  string  `json:"submitter"`
	Category   string  `json:"category"`
	Amount     float64 `json:"amount"`
	ReceiptURL string  `json:"receiptUrl"`
}

// Decision is both the HITL event payload (raised by ApproveExpense) and the
// auto-approval value.
type Decision struct {
	Approved bool   `json:"approved"`
	By       string `json:"by"`
}

// ExpenseResult is the orchestration output.
type ExpenseResult struct {
	ExpenseID string `json:"expenseId"`
	Status    string `json:"status"` // "approved" | "rejected"
	Note      string `json:"note"`
}

// ProcessExpense validates an expense with three checks running in parallel
// (fan-out/fan-in), reports progress via custom status, and — for larger
// amounts — waits for a human approval event before finishing.
func ProcessExpense(ctx *task.OrchestrationContext) (any, error) {
	var exp Expense
	if err := ctx.GetInput(&exp); err != nil {
		return nil, err
	}

	// Progress is reported via custom status, which surfaces in the status
	// endpoint (client.GetStatus → OrchestrationStatus.CustomStatus).
	ctx.SetCustomStatus("validating")

	// Fan-out: schedule all three checks before awaiting any, so they run
	// concurrently. Fan-in: await each result.
	receiptTask := ctx.CallActivity("ValidateReceipt", task.WithActivityInput(exp))
	policyTask := ctx.CallActivity("CheckPolicy", task.WithActivityInput(exp))
	budgetTask := ctx.CallActivity("CheckBudget", task.WithActivityInput(exp))

	var receiptOK, policyOK, budgetOK bool
	if err := receiptTask.Await(&receiptOK); err != nil {
		return nil, err
	}
	if err := policyTask.Await(&policyOK); err != nil {
		return nil, err
	}
	if err := budgetTask.Await(&budgetOK); err != nil {
		return nil, err
	}
	if !receiptOK || !policyOK || !budgetOK {
		ctx.SetCustomStatus("rejected: failed automated validation")
		return finalizeExpense(ctx, exp, "rejected", "failed automated validation")
	}

	// Small amounts auto-approve; larger amounts require a human.
	decision := Decision{Approved: true, By: "auto"}
	if exp.Amount > autoApproveLimit {
		ctx.SetCustomStatus("awaiting manager approval")

		// HITL: block until an "ApprovalDecision" external event arrives or
		// the durable timeout fires. On timeout, Await returns an error and
		// we treat the request as expired.
		var human Decision
		if err := ctx.WaitForSingleEvent("ApprovalDecision", approvalTimeout).Await(&human); err != nil {
			ctx.SetCustomStatus("rejected: approval timed out")
			return finalizeExpense(ctx, exp, "rejected", "approval timed out")
		}
		decision = human
	}

	status := "approved"
	if !decision.Approved {
		status = "rejected"
	}
	ctx.SetCustomStatus(status + " by " + decision.By)
	return finalizeExpense(ctx, exp, status, "decided by "+decision.By)
}

// finalizeExpense records the outcome via an activity and returns the result.
func finalizeExpense(ctx *task.OrchestrationContext, exp Expense, status, note string) (any, error) {
	result := ExpenseResult{ExpenseID: exp.ID, Status: status, Note: note}
	if err := ctx.CallActivity("RecordDecision", task.WithActivityInput(result)).Await(nil); err != nil {
		return nil, err
	}
	return result, nil
}

// --- ProcessExpense activities (durabletask-go native signatures) ---

func ValidateReceipt(ctx task.ActivityContext) (any, error) {
	var exp Expense
	if err := ctx.GetInput(&exp); err != nil {
		return nil, err
	}
	return exp.ReceiptURL != "", nil
}

func CheckPolicy(ctx task.ActivityContext) (any, error) {
	var exp Expense
	if err := ctx.GetInput(&exp); err != nil {
		return nil, err
	}
	return exp.Category != "", nil
}

func CheckBudget(ctx task.ActivityContext) (any, error) {
	var exp Expense
	if err := ctx.GetInput(&exp); err != nil {
		return nil, err
	}
	return exp.Amount <= 10000, nil
}

func RecordDecision(ctx task.ActivityContext) (any, error) {
	var r ExpenseResult
	if err := ctx.GetInput(&r); err != nil {
		return nil, err
	}
	slog.InfoContext(ctx.Context(), "expense decision recorded",
		"expense_id", r.ExpenseID, "status", r.Status, "note", r.Note)
	return "recorded", nil
}

// =============================================================================
// HTTP endpoints — start, status/progress, and the HITL response.
// =============================================================================

// StartHelloCities (POST /api/hello) starts the simple orchestration and
// returns just the new instance ID.
func StartHelloCities(w http.ResponseWriter, r *http.Request) {
	client, ok := durabletask.ClientFromContext(r.Context())
	if !ok {
		http.Error(w, "durable client unavailable", http.StatusInternalServerError)
		return
	}
	id, err := client.ScheduleNewOrchestration(r.Context(), "HelloCities", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"id": id})
}

// SubmitExpense (POST /api/expenses) starts the expense orchestration and
// returns a 202 with a pointer to this app's own status route.
func SubmitExpense(w http.ResponseWriter, r *http.Request) {
	client, ok := durabletask.ClientFromContext(r.Context())
	if !ok {
		http.Error(w, "durable client unavailable", http.StatusInternalServerError)
		return
	}
	var exp Expense
	if err := json.NewDecoder(r.Body).Decode(&exp); err != nil {
		http.Error(w, "invalid expense: "+err.Error(), http.StatusBadRequest)
		return
	}
	id, err := client.ScheduleNewOrchestration(r.Context(), "ProcessExpense", exp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Location", "/api/expenses/"+id)
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"id":                id,
		"statusQueryGetUri": "/api/expenses/" + id,
	})
}

// GetExpenseStatus (GET /api/expenses/{id}) returns the orchestration's
// runtime status. The orchestrator's custom status (set via SetCustomStatus)
// surfaces here as the progress channel.
func GetExpenseStatus(w http.ResponseWriter, r *http.Request) {
	client, ok := durabletask.ClientFromContext(r.Context())
	if !ok {
		http.Error(w, "durable client unavailable", http.StatusInternalServerError)
		return
	}
	id := instanceIDFromPath(r, "expenses")
	status, err := client.GetStatus(r.Context(), id)
	if err == durabletask.ErrInstanceNotFound {
		http.Error(w, "no such instance", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(status)
}

// ApproveExpense (POST /api/expenses/{id}/approve) is the HITL response: it
// raises the "ApprovalDecision" external event the orchestration is waiting
// on. The JSON body is the Decision payload.
func ApproveExpense(w http.ResponseWriter, r *http.Request) {
	client, ok := durabletask.ClientFromContext(r.Context())
	if !ok {
		http.Error(w, "durable client unavailable", http.StatusInternalServerError)
		return
	}
	id := instanceIDFromPath(r, "expenses")

	var decision Decision
	if err := json.NewDecoder(r.Body).Decode(&decision); err != nil {
		http.Error(w, "invalid decision: "+err.Error(), http.StatusBadRequest)
		return
	}
	if decision.By == "" {
		decision.By = "manager"
	}
	if err := client.RaiseEvent(r.Context(), id, "ApprovalDecision", decision); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// instanceIDFromPath extracts the path segment after resource, e.g. for the
// route "expenses/{id}" and path "/api/expenses/abc-123" it returns
// "abc-123".
func instanceIDFromPath(r *http.Request, resource string) string {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	for i, p := range parts {
		if p == resource && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func main() {
	app := sdk.FunctionApp()

	// One registration wires the whole durable feature: it injects the
	// durable client into starter invocations and contributes the work-item
	// listener (executes orchestrators + activities) that the worker starts
	// and stops automatically. Add as many orchestrations as you like.
	app.Use(durabletask.Middleware(
		// Orchestrators.
		durabletask.WithOrchestrator("HelloCities", HelloCities),
		durabletask.WithOrchestrator("ProcessExpense", ProcessExpense),
		// Activities.
		durabletask.WithActivity("SayHello", SayHello),
		durabletask.WithActivity("ValidateReceipt", ValidateReceipt),
		durabletask.WithActivity("CheckPolicy", CheckPolicy),
		durabletask.WithActivity("CheckBudget", CheckBudget),
		durabletask.WithActivity("RecordDecision", RecordDecision),
	))

	// Management endpoints are ordinary HTTP functions.
	app.HTTP("StartHelloCities", StartHelloCities,
		sdk.WithMethods("post"), sdk.WithRoute("hello"), sdk.WithAuth("anonymous"))
	app.HTTP("SubmitExpense", SubmitExpense,
		sdk.WithMethods("post"), sdk.WithRoute("expenses"), sdk.WithAuth("anonymous"))
	app.HTTP("GetExpenseStatus", GetExpenseStatus,
		sdk.WithMethods("get"), sdk.WithRoute("expenses/{id}"), sdk.WithAuth("anonymous"))
	app.HTTP("ApproveExpense", ApproveExpense,
		sdk.WithMethods("post"), sdk.WithRoute("expenses/{id}/approve"), sdk.WithAuth("anonymous"))

	worker.Start(app)
}
