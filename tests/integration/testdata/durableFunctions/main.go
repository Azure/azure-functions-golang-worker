// Command durableFunctions is the integration fixture for Durable Functions.
//
// It mirrors samples/durableFunctions minus the OpenTelemetry wiring, which the
// durable assertions do not exercise and which would otherwise pull the
// collector and its exporters into this module.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/azure/azure-functions-golang-worker/middleware/durabletask"
	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/task"
)

const (
	// autoApproveLimit is the amount (inclusive) below which expenses are
	// approved automatically. Above it a human must approve.
	autoApproveLimit = 100.0

	// approvalTimeout is how long ProcessExpense waits for a human decision
	// before auto-rejecting.
	approvalTimeout = 72 * time.Hour
)

// HelloCities calls SayHello for each city in sequence.
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

// SayHello proves activity inputs arrive decoded rather than JSON quoted.
func SayHello(_ context.Context, city string) (string, error) {
	return fmt.Sprintf("Hello, %s!", city), nil
}

// Expense is the ProcessExpense input.
type Expense struct {
	ID         string  `json:"id"`
	Submitter  string  `json:"submitter"`
	Category   string  `json:"category"`
	Amount     float64 `json:"amount"`
	ReceiptURL string  `json:"receiptUrl"`
}

// Decision is the approval event payload and the auto-approval value.
type Decision struct {
	Approved bool   `json:"approved"`
	By       string `json:"by"`
}

// ExpenseResult is the ProcessExpense output.
type ExpenseResult struct {
	ExpenseID string `json:"expenseId"`
	Status    string `json:"status"`
	Note      string `json:"note"`
}

// ProcessExpense fans out three validations, reports progress through custom
// status, and waits for a human decision on larger amounts.
func ProcessExpense(ctx *task.OrchestrationContext) (any, error) {
	var exp Expense
	if err := ctx.GetInput(&exp); err != nil {
		return nil, err
	}

	ctx.SetCustomStatus("validating")

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

	decision := Decision{Approved: true, By: "auto"}
	if exp.Amount > autoApproveLimit {
		ctx.SetCustomStatus("awaiting manager approval")

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

func finalizeExpense(ctx *task.OrchestrationContext, exp Expense, status, note string) (any, error) {
	result := ExpenseResult{ExpenseID: exp.ID, Status: status, Note: note}
	if err := ctx.CallActivity("RecordDecision", task.WithActivityInput(result)).Await(nil); err != nil {
		return nil, err
	}
	return result, nil
}

func ValidateReceipt(_ context.Context, exp Expense) (bool, error) { return exp.ReceiptURL != "", nil }

func CheckPolicy(_ context.Context, exp Expense) (bool, error) { return exp.Category != "", nil }

func CheckBudget(_ context.Context, exp Expense) (bool, error) { return exp.Amount <= 10000, nil }

func RecordDecision(ctx context.Context, r ExpenseResult) (string, error) {
	slog.InfoContext(ctx, "expense decision recorded",
		"expense_id", r.ExpenseID, "status", r.Status, "note", r.Note)
	return "recorded", nil
}

// StartHelloCities returns only the instance ID, the minimal starter shape.
func StartHelloCities(w http.ResponseWriter, r *http.Request) {
	client, ok := durabletask.ClientFromContext(r.Context())
	if !ok {
		http.Error(w, "durable client unavailable", http.StatusInternalServerError)
		return
	}
	id, err := client.ScheduleNewOrchestration(r.Context(), "HelloCities")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"id": string(id)})
}

// SubmitExpense returns the canonical check-status reply.
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
	id, err := client.ScheduleNewOrchestration(r.Context(), "ProcessExpense", api.WithInput(exp))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = client.WriteCheckStatusResponse(w, r, string(id))
}

// GetExpenseStatus resolves any instance ID, not only expense orchestrations.
func GetExpenseStatus(w http.ResponseWriter, r *http.Request) {
	client, ok := durabletask.ClientFromContext(r.Context())
	if !ok {
		http.Error(w, "durable client unavailable", http.StatusInternalServerError)
		return
	}
	meta, err := client.FetchOrchestrationMetadata(r.Context(),
		api.InstanceID(instanceIDFromPath(r, "expenses")))
	if errors.Is(err, api.ErrInstanceNotFound) {
		http.Error(w, "no such instance", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = durabletask.WriteStatusResponse(w, meta)
}

// ApproveExpense raises the ApprovalDecision event the orchestration awaits.
func ApproveExpense(w http.ResponseWriter, r *http.Request) {
	client, ok := durabletask.ClientFromContext(r.Context())
	if !ok {
		http.Error(w, "durable client unavailable", http.StatusInternalServerError)
		return
	}
	var decision Decision
	if err := json.NewDecoder(r.Body).Decode(&decision); err != nil {
		http.Error(w, "invalid decision: "+err.Error(), http.StatusBadRequest)
		return
	}
	if decision.By == "" {
		decision.By = "manager"
	}
	if err := client.RaiseEvent(r.Context(), api.InstanceID(instanceIDFromPath(r, "expenses")),
		"ApprovalDecision", api.WithEventPayload(decision)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// instanceIDFromPath returns the path segment following resource.
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

	durable := durabletask.Middleware()

	durable.Orchestrator("HelloCities", HelloCities)
	durable.Orchestrator("ProcessExpense", ProcessExpense)

	durable.Activity("SayHello", SayHello)
	durable.Activity("ValidateReceipt", ValidateReceipt)
	durable.Activity("CheckPolicy", CheckPolicy)
	durable.Activity("CheckBudget", CheckBudget)
	durable.Activity("RecordDecision", RecordDecision)

	app.Use(durable)

	app.HTTP("StartHelloCities", StartHelloCities,
		sdk.WithMethods("post"), sdk.WithRoute("hello"), sdk.WithAuth("anonymous"),
		durabletask.ClientInput())
	app.HTTP("SubmitExpense", SubmitExpense,
		sdk.WithMethods("post"), sdk.WithRoute("expenses"), sdk.WithAuth("anonymous"),
		durabletask.ClientInput())
	app.HTTP("GetExpenseStatus", GetExpenseStatus,
		sdk.WithMethods("get"), sdk.WithRoute("expenses/{id}"), sdk.WithAuth("anonymous"),
		durabletask.ClientInput())
	app.HTTP("ApproveExpense", ApproveExpense,
		sdk.WithMethods("post"), sdk.WithRoute("expenses/{id}/approve"), sdk.WithAuth("anonymous"),
		durabletask.ClientInput())

	worker.Start(app)
}
