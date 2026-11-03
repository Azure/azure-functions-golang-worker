// Command durableFunctions is the integration fixture for Durable Functions.
//
// It mirrors samples/durableFunctions without an embedded collector. The
// opt-in ordering probe uses an in-memory OpenTelemetry exporter.
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

// Counter is a stateful entity used by the integration tests and sample.
func Counter(ctx *durabletask.EntityContext) (any, error) {
	count := 0
	if ctx.HasState() {
		if err := ctx.GetState(&count); err != nil {
			return nil, err
		}
	}

	switch ctx.OperationName {
	case "add":
		var increment int
		if err := ctx.GetInput(&increment); err != nil {
			return nil, err
		}
		count += increment
		if err := ctx.SetState(count); err != nil {
			return nil, err
		}
		return count, nil
	case "get":
		return count, nil
	case "fail":
		return nil, errors.New("intentional entity failure")
	default:
		return nil, fmt.Errorf("unknown counter operation %q", ctx.OperationName)
	}
}

// EntitySignals sends both immediate and scheduled signals, then takes a
// critical section and calls the entity to verify durable state and replay.
func EntitySignals(ctx *task.OrchestrationContext) (any, error) {
	id := api.NewEntityID("counter", string(ctx.ID))
	if err := ctx.SignalEntity(id, "add", task.WithSignalEntityInput(2)); err != nil {
		return nil, err
	}
	due := ctx.CurrentTimeUtc.Add(2 * time.Second)
	if err := ctx.SignalEntity(id, "add",
		task.WithSignalEntityInput(3),
		task.WithSignalEntityScheduledTime(due)); err != nil {
		return nil, err
	}
	if err := ctx.CreateTimer(due.Add(time.Second).Sub(ctx.CurrentTimeUtc)).Await(nil); err != nil {
		return nil, err
	}

	release, err := ctx.LockEntities(id)
	if err != nil {
		return nil, err
	}
	defer release()

	var count int
	if err := ctx.CallEntity(id, "get").Await(&count); err != nil {
		return nil, err
	}
	return count, nil
}

func EntityFailure(ctx *task.OrchestrationContext) (any, error) {
	id := api.NewEntityID("counter", string(ctx.ID))
	err := ctx.CallEntity(id, "fail").Await(nil)
	if err == nil {
		return "entity call unexpectedly succeeded", nil
	}
	return err.Error(), nil
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

func StartEntitySignals(w http.ResponseWriter, r *http.Request) {
	startOrchestration(w, r, "EntitySignals")
}

func StartEntityFailure(w http.ResponseWriter, r *http.Request) {
	startOrchestration(w, r, "EntityFailure")
}

func EntityManagement(w http.ResponseWriter, r *http.Request) {
	client, ok := durabletask.ClientFromContext(r.Context())
	if !ok {
		http.Error(w, "durable client unavailable", http.StatusInternalServerError)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	id := api.NewEntityID("counter", fmt.Sprintf("management-%d", time.Now().UnixNano()))
	if err := client.SignalEntity(ctx, id, "add", api.WithSignalInput(7)); err != nil {
		http.Error(w, "signal entity: "+err.Error(), http.StatusInternalServerError)
		return
	}

	var state int
	for {
		metadata, err := client.GetEntity(ctx, id)
		if err != nil {
			http.Error(w, "get entity: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if metadata != nil && metadata.HasState {
			if err := metadata.ReadState(&state); err != nil {
				http.Error(w, "read entity state: "+err.Error(), http.StatusInternalServerError)
				return
			}
			if state == 7 {
				break
			}
		}
		select {
		case <-ctx.Done():
			http.Error(w, "timed out waiting for entity signal", http.StatusGatewayTimeout)
			return
		case <-time.After(25 * time.Millisecond):
		}
	}

	results, err := client.QueryEntities(ctx, api.EntityQuery{
		InstanceIDStartsWith: id.String(),
		PageSize:             10,
	})
	if err != nil {
		http.Error(w, "query entities: "+err.Error(), http.StatusInternalServerError)
		return
	}
	for _, metadata := range results.Entities {
		if metadata.InstanceID == id {
			var queriedState int
			if err := metadata.ReadState(&queriedState); err != nil {
				http.Error(w, "read queried entity state: "+err.Error(), http.StatusInternalServerError)
				return
			}
			if queriedState == 7 {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]int{"state": state, "queryState": queriedState})
				return
			}
		}
	}
	http.Error(w, "entity query did not return the signaled entity state", http.StatusInternalServerError)
}

func startOrchestration(w http.ResponseWriter, r *http.Request, name string) {
	client, ok := durabletask.ClientFromContext(r.Context())
	if !ok {
		http.Error(w, "durable client unavailable", http.StatusInternalServerError)
		return
	}
	id, err := client.ScheduleNewOrchestration(r.Context(), name)
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
	payloadStore, err := newBlobPayloadStore()
	if err != nil {
		panic(err)
	}
	if payloadStore != nil {
		durable = durabletask.Middleware(durabletask.WithLargePayloads(payloadStore.options))
		registerBlobOffloading(app, durable, payloadStore)
	}

	durable.Orchestrator("HelloCities", HelloCities)
	durable.Orchestrator("ProcessExpense", ProcessExpense)
	durable.Orchestrator("EntitySignals", EntitySignals)
	durable.Orchestrator("EntityFailure", EntityFailure)

	durable.Activity("SayHello", SayHello)
	durable.Activity("ValidateReceipt", ValidateReceipt)
	durable.Activity("CheckPolicy", CheckPolicy)
	durable.Activity("CheckBudget", CheckBudget)
	durable.Activity("RecordDecision", RecordDecision)
	durable.Entity("counter", Counter)

	configureLifecycleProbe(app, durable)
	if !configureOrderingProbe(app, durable) {
		app.Use(durable)
	}

	app.HTTP("StartHelloCities", StartHelloCities,
		sdk.WithMethods("post"), sdk.WithRoute("hello"), sdk.WithAuth("anonymous"),
		durabletask.ClientInput())
	app.HTTP("StartEntitySignals", StartEntitySignals,
		sdk.WithMethods("post"), sdk.WithRoute("entities"), sdk.WithAuth("anonymous"),
		durabletask.ClientInput())
	app.HTTP("StartEntityFailure", StartEntityFailure,
		sdk.WithMethods("post"), sdk.WithRoute("entity-failure"), sdk.WithAuth("anonymous"),
		durabletask.ClientInput())
	app.HTTP("EntityManagement", EntityManagement,
		sdk.WithMethods("post"), sdk.WithRoute("entity-management"), sdk.WithAuth("anonymous"),
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
