package integration

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// durableBaseURLEnv points the suite at an app that is already running instead
// of starting a local Core Tools host. Set it to run the same checks against a
// deployed Function App:
//
//	DURABLE_E2E_BASE_URL=https://myapp.azurewebsites.net go test -run TestDurable ./...
//
// Assertions go through the fixture's anonymous HTTP endpoints, including its
// test-only lifecycle probes, not the host's HTTP management API.
const durableBaseURLEnv = "DURABLE_E2E_BASE_URL"

// Durable orchestrations round-trip through storage, so they need noticeably
// more time than a single HTTP invocation.
const (
	durableStartTimeout    = 90 * time.Second
	durableCompleteTimeout = 60 * time.Second
	durablePollInterval    = 500 * time.Millisecond
)

// autoApproveLimit mirrors the sample: expenses above it require a human
// decision, expenses at or below it approve automatically.
const autoApproveLimit = 100.0

type expense struct {
	ID         string  `json:"id"`
	Submitter  string  `json:"submitter"`
	Category   string  `json:"category"`
	Amount     float64 `json:"amount"`
	ReceiptURL string  `json:"receiptUrl"`
}

type decision struct {
	Approved bool   `json:"approved"`
	By       string `json:"by"`
}

type expenseResult struct {
	ExpenseID string `json:"expenseId"`
	Status    string `json:"status"`
	Note      string `json:"note"`
}

// orchestrationStatus mirrors durabletask.StatusResponse, the standard Durable
// Functions status payload the sample's endpoint returns. Input, Output and
// CustomStatus are raw JSON because the payload embeds the orchestration's
// serialized values as-is, so an object stays an object rather than becoming a
// quoted string.
type orchestrationStatus struct {
	InstanceID    string          `json:"instanceId"`
	Name          string          `json:"name"`
	RuntimeStatus string          `json:"runtimeStatus"`
	Input         json.RawMessage `json:"input"`
	Output        json.RawMessage `json:"output"`
	CustomStatus  json.RawMessage `json:"customStatus"`
}

// durableApp drives the durableFunctions fixture over HTTP. Only suite-owned
// local hosts can be restarted.
type durableApp struct {
	t       *testing.T
	baseURL string
	restart func(*testing.T) string
}

var durableHTTPClient = &http.Client{Timeout: 70 * time.Second}

// startDurableApp returns a driver for the durable app. It reuses an
// already-running app when durableBaseURLEnv is set, and otherwise starts the
// durable fixture under a local Core Tools host with Azurite as the state store.
func startDurableApp(t *testing.T) *durableApp {
	t.Helper()

	if baseURL := os.Getenv(durableBaseURLEnv); baseURL != "" {
		t.Logf("using existing durable app at %s", baseURL)
		return &durableApp{t: t, baseURL: strings.TrimSuffix(baseURL, "/")}
	}

	host, restart := startDurableHost(t, nil)
	return &durableApp{t: t, baseURL: host.URL(), restart: func(t *testing.T) string {
		return restart(t).URL()
	}}
}

// post sends a JSON request and returns the status code and body. A nil
// payload sends an empty body.
func (a *durableApp) post(path string, payload any) (int, []byte) {
	a.t.Helper()

	var body *bytes.Reader
	if payload == nil {
		body = bytes.NewReader(nil)
	} else {
		encoded, err := json.Marshal(payload)
		if err != nil {
			a.t.Fatalf("encode %s payload: %v", path, err)
		}
		body = bytes.NewReader(encoded)
	}

	resp, err := durableHTTPClient.Post(a.baseURL+path, "application/json", body)
	if err != nil {
		a.t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode, readAll(a.t, resp.Body)
}

// unsupportedRuntimeSymptoms are the two ways a DurableTask extension without
// native worker runtime support fails, both of which surface as a 500 from a
// starter endpoint:
//
//   - The extension does not recognize the runtime at all, so no durableClient
//     binding value reaches the worker.
//   - The extension falls back to its legacy local HTTP RPC endpoint instead of
//     the gRPC one, so the durable client's gRPC handshake gets an HTTP
//     response back.
var unsupportedRuntimeSymptoms = []string{
	"durable client unavailable",
	"not a settings frame",
}

// scheduleOrchestration posts to a starter endpoint and returns the new
// instance ID.
func (a *durableApp) scheduleOrchestration(path string, payload any) string {
	a.t.Helper()

	status, body := a.post(path, payload)
	if status == http.StatusInternalServerError {
		for _, symptom := range unsupportedRuntimeSymptoms {
			if !strings.Contains(string(body), symptom) {
				continue
			}
			a.t.Fatalf("POST %s failed because the durable client could not reach the host: %s\n\n"+
				"This is what a DurableTask extension without native worker runtime support looks like. "+
				"The extension has to recognize FUNCTIONS_WORKER_RUNTIME=native and select the gRPC durable "+
				"protocol, which landed in DurableTask 3.15.0. Check that the extension bundle the host loaded "+
				"carries 3.15.0 or later.",
				path, body)
		}
	}
	if status != http.StatusAccepted {
		a.t.Fatalf("POST %s: expected status 202, got %d: %s", path, status, body)
	}

	var started struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &started); err != nil {
		a.t.Fatalf("decode %s response %q: %v", path, body, err)
	}
	if started.ID == "" {
		a.t.Fatalf("POST %s returned an empty instance ID: %s", path, body)
	}
	return started.ID
}

// status reads an orchestration's current state. The sample's status endpoint
// resolves any instance ID, not just expense orchestrations.
func (a *durableApp) status(instanceID string) orchestrationStatus {
	a.t.Helper()

	resp, err := durableHTTPClient.Get(a.baseURL + "/api/expenses/" + instanceID)
	if err != nil {
		a.t.Fatalf("GET status for %s: %v", instanceID, err)
	}
	defer resp.Body.Close()
	body := readAll(a.t, resp.Body)

	if resp.StatusCode != http.StatusOK {
		a.t.Fatalf("GET status for %s: expected status 200, got %d: %s", instanceID, resp.StatusCode, body)
	}

	var status orchestrationStatus
	if err := json.Unmarshal(body, &status); err != nil {
		a.t.Fatalf("decode status %q: %v", body, err)
	}
	return status
}

// approve raises the ApprovalDecision event the orchestration is waiting on.
func (a *durableApp) approve(instanceID string, d decision) {
	a.t.Helper()

	path := "/api/expenses/" + instanceID + "/approve"
	if status, body := a.post(path, d); status != http.StatusAccepted {
		a.t.Fatalf("POST %s: expected status 202, got %d: %s", path, status, body)
	}
}

// waitForStatus polls until match reports success, and fails with the last
// observed state so a timeout explains where the orchestration stalled.
func (a *durableApp) waitForStatus(instanceID, describe string, timeout time.Duration, match func(orchestrationStatus) bool) orchestrationStatus {
	a.t.Helper()

	deadline := time.Now().Add(timeout)
	var last orchestrationStatus
	for {
		last = a.status(instanceID)
		if match(last) {
			return last
		}
		// Terminal states never recover, so stop early instead of burning the
		// full timeout.
		if last.RuntimeStatus == "Failed" || last.RuntimeStatus == "Terminated" {
			a.t.Fatalf("orchestration %s reached %s while waiting for %s: output=%s",
				instanceID, last.RuntimeStatus, describe, last.Output)
		}
		if time.Now().After(deadline) {
			a.t.Fatalf("timed out after %s waiting for %s on %s: runtimeStatus=%q customStatus=%q",
				timeout, describe, instanceID, last.RuntimeStatus, last.CustomStatus)
		}
		time.Sleep(durablePollInterval)
	}
}

func (a *durableApp) waitForCompletion(instanceID string) orchestrationStatus {
	a.t.Helper()
	return a.waitForStatus(instanceID, "completion", durableCompleteTimeout, func(s orchestrationStatus) bool {
		return s.RuntimeStatus == "Completed"
	})
}

func (a *durableApp) managementStatus(operation, instanceID string) orchestrationStatus {
	a.t.Helper()
	code, body := a.post("/api/lifecycle/"+operation, map[string]string{"id": instanceID})
	if code != http.StatusOK {
		a.t.Fatalf("%s %s: status=%d body=%s", operation, instanceID, code, body)
	}
	var status orchestrationStatus
	if err := json.Unmarshal(body, &status); err != nil {
		a.t.Fatalf("decode management status: %v", err)
	}
	if status.InstanceID != instanceID {
		a.t.Fatalf("%s returned instance %q, want %q", operation, status.InstanceID, instanceID)
	}
	return status
}

// decodeExpenseResult unwraps the orchestration output, which the status
// payload embeds as raw JSON.
func decodeExpenseResult(t *testing.T, output json.RawMessage) expenseResult {
	t.Helper()

	var result expenseResult
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode expense result %s: %v", output, err)
	}
	return result
}

// customStatusText reads the orchestration's custom status, which the payload
// embeds as raw JSON (a JSON string for the sample's textual progress).
func customStatusText(raw json.RawMessage) string {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return ""
	}
	return text
}

func newExpense(t *testing.T, amount float64) expense {
	t.Helper()
	return expense{
		ID:         fmt.Sprintf("E-%s", strings.ReplaceAll(t.Name(), "/", "-")),
		Submitter:  "integration-test",
		Category:   "travel",
		Amount:     amount,
		ReceiptURL: "https://example.invalid/receipt.pdf",
	}
}

// TestDurableOrchestrations exercises the Durable Functions programming model
// end to end against a real Functions host: the durable client binding, the
// gRPC management client, orchestration replay, activity dispatch, custom
// status, and external events.
//
// The subtests share one host because they exercise the same app and each
// orchestration is isolated by its own instance ID.
func TestDurableOrchestrations(t *testing.T) {
	app := startDurableApp(t)

	t.Run("SequentialOrchestrationCompletes", func(t *testing.T) {
		app := &durableApp{t: t, baseURL: app.baseURL}

		instanceID := app.scheduleOrchestration("/api/hello", nil)
		status := app.waitForCompletion(instanceID)

		if status.Name != "HelloCities" {
			t.Errorf("expected orchestration name %q, got %q", "HelloCities", status.Name)
		}

		var greetings []string
		if err := json.Unmarshal(status.Output, &greetings); err != nil {
			t.Fatalf("decode greetings %s: %v", status.Output, err)
		}
		// Activity inputs arrive JSON-encoded; the worker decodes them before
		// binding, so the city must not carry its quotes into the greeting.
		want := []string{"Hello, Tokyo!", "Hello, Seattle!", "Hello, London!"}
		if len(greetings) != len(want) {
			t.Fatalf("expected %d greetings, got %d: %v", len(want), len(greetings), greetings)
		}
		for i, greeting := range want {
			if greetings[i] != greeting {
				t.Errorf("greeting %d: expected %q, got %q", i, greeting, greetings[i])
			}
		}
	})

	t.Run("FanOutAutoApproves", func(t *testing.T) {
		app := &durableApp{t: t, baseURL: app.baseURL}

		// At or below the limit the orchestration never waits for a human.
		instanceID := app.scheduleOrchestration("/api/expenses", newExpense(t, autoApproveLimit-1))
		status := app.waitForCompletion(instanceID)

		if status.Name != "ProcessExpense" {
			t.Errorf("expected orchestration name %q, got %q", "ProcessExpense", status.Name)
		}
		result := decodeExpenseResult(t, status.Output)
		if result.Status != "approved" {
			t.Errorf("expected status %q, got %q (note: %s)", "approved", result.Status, result.Note)
		}
		if !strings.Contains(result.Note, "auto") {
			t.Errorf("expected an automatic decision, got note %q", result.Note)
		}
	})

	t.Run("HumanApprovalUnblocksOrchestration", func(t *testing.T) {
		app := &durableApp{t: t, baseURL: app.baseURL}

		instanceID := app.scheduleOrchestration("/api/expenses", newExpense(t, autoApproveLimit+650))

		// Custom status is the orchestration's progress channel and proves the
		// orchestrator parked on the external event rather than finishing.
		app.waitForStatus(instanceID, "the approval wait", durableCompleteTimeout, func(s orchestrationStatus) bool {
			return customStatusText(s.CustomStatus) == "awaiting manager approval"
		})
		if status := app.status(instanceID); status.RuntimeStatus != "Running" {
			t.Fatalf("expected the orchestration to still be Running while awaiting approval, got %q", status.RuntimeStatus)
		}

		app.approve(instanceID, decision{Approved: true, By: "integration-test"})

		status := app.waitForCompletion(instanceID)
		result := decodeExpenseResult(t, status.Output)
		if result.Status != "approved" {
			t.Errorf("expected status %q, got %q (note: %s)", "approved", result.Status, result.Note)
		}
		if !strings.Contains(result.Note, "integration-test") {
			t.Errorf("expected the decision to record the approver, got note %q", result.Note)
		}
	})

	t.Run("HumanRejectionIsRecorded", func(t *testing.T) {
		app := &durableApp{t: t, baseURL: app.baseURL}

		instanceID := app.scheduleOrchestration("/api/expenses", newExpense(t, autoApproveLimit+900))
		app.waitForStatus(instanceID, "the approval wait", durableCompleteTimeout, func(s orchestrationStatus) bool {
			return customStatusText(s.CustomStatus) == "awaiting manager approval"
		})

		app.approve(instanceID, decision{Approved: false, By: "integration-test"})

		status := app.waitForCompletion(instanceID)
		result := decodeExpenseResult(t, status.Output)
		if result.Status != "rejected" {
			t.Errorf("expected status %q, got %q (note: %s)", "rejected", result.Status, result.Note)
		}
	})

	t.Run("FailedValidationRejectsWithoutApproval", func(t *testing.T) {
		app := &durableApp{t: t, baseURL: app.baseURL}

		// An empty receipt URL fails ValidateReceipt, so the orchestration
		// rejects during fan-in and never reaches the approval wait.
		exp := newExpense(t, autoApproveLimit+500)
		exp.ReceiptURL = ""

		instanceID := app.scheduleOrchestration("/api/expenses", exp)
		status := app.waitForCompletion(instanceID)

		result := decodeExpenseResult(t, status.Output)
		if result.Status != "rejected" {
			t.Errorf("expected status %q, got %q (note: %s)", "rejected", result.Status, result.Note)
		}
		if !strings.Contains(result.Note, "validation") {
			t.Errorf("expected an automated validation failure, got note %q", result.Note)
		}
	})

	t.Run("UnknownInstanceReturnsNotFound", func(t *testing.T) {
		resp, err := durableHTTPClient.Get(app.baseURL + "/api/expenses/does-not-exist")
		if err != nil {
			t.Fatalf("GET status for an unknown instance: %v", err)
		}
		defer resp.Body.Close()
		body := readAll(t, resp.Body)

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("expected status 404, got %d: %s", resp.StatusCode, body)
		}
	})

	t.Run("DirectGoManagementOperations", func(t *testing.T) {
		app := &durableApp{t: t, baseURL: app.baseURL}
		wantID := "management-" + rand.Text()
		id := app.scheduleOrchestration("/api/lifecycle/schedule", map[string]any{
			"id": wantID, "name": "LifecycleWait", "input": 60,
		})
		if id != wantID {
			t.Fatalf("scheduled ID = %q, want explicit %q", id, wantID)
		}
		started := app.managementStatus("wait-start", id)
		if started.RuntimeStatus != "Running" || started.Name != "LifecycleWait" {
			t.Fatalf("unexpected start metadata: %+v", started)
		}
		app.waitForStatus(id, "custom status while waiting", durableCompleteTimeout, func(s orchestrationStatus) bool {
			return s.RuntimeStatus == "Running" && customStatusText(s.CustomStatus) == "waiting for Continue"
		})
		fetched := app.managementStatus("fetch", id)
		if string(fetched.Input) != "60" || customStatusText(fetched.CustomStatus) != "waiting for Continue" {
			t.Fatalf("unexpected fetched metadata: %+v", fetched)
		}
		code, body := app.post("/api/lifecycle/raise", map[string]any{
			"id": id, "event": "Continue", "payload": "approved",
		})
		if code != http.StatusAccepted {
			t.Fatalf("raise: status=%d body=%s", code, body)
		}
		final := app.managementStatus("wait-completion", id)
		if final.RuntimeStatus != "Completed" || string(final.Output) != `"approved"` ||
			customStatusText(final.CustomStatus) != "event received" {
			t.Fatalf("unexpected completion metadata: %+v", final)
		}
		code, body = app.post("/api/lifecycle/fetch", map[string]string{"id": "missing-" + rand.Text()})
		if code != http.StatusNotFound || !strings.Contains(string(body), "api.ErrInstanceNotFound") {
			t.Fatalf("missing instance: status=%d body=%s", code, body)
		}
	})

	t.Run("ExternalEventTimeoutUsesDurableTimer", func(t *testing.T) {
		app := &durableApp{t: t, baseURL: app.baseURL}
		start := time.Now()
		id := app.scheduleOrchestration("/api/lifecycle/schedule", map[string]any{
			"id": "timeout-" + rand.Text(), "name": "LifecycleWait", "input": 3,
		})
		final := app.managementStatus("wait-completion", id)
		if final.RuntimeStatus != "Completed" || string(final.Output) != `"timed out"` ||
			customStatusText(final.CustomStatus) != "timed out" {
			t.Fatalf("unexpected timeout metadata: %+v", final)
		}
		if elapsed := time.Since(start); elapsed < 3*time.Second {
			t.Fatalf("external event timed out before its durable timer: %s", elapsed)
		}
	})

	t.Run("EarlyExternalEventIsNotLost", func(t *testing.T) {
		app := &durableApp{t: t, baseURL: app.baseURL}
		id := app.scheduleOrchestration("/api/lifecycle/schedule", map[string]any{
			"id": "early-" + rand.Text(), "name": "BufferedEvent",
		})
		code, body := app.post("/api/lifecycle/raise", map[string]any{
			"id": id, "event": "Continue", "payload": "early event",
		})
		if code != http.StatusAccepted {
			t.Fatalf("early event: status=%d body=%s", code, body)
		}
		app.waitForStatus(id, "Release gate before consuming Continue", durableCompleteTimeout, func(s orchestrationStatus) bool {
			return s.RuntimeStatus == "Running" && customStatusText(s.CustomStatus) == "waiting for Release"
		})
		code, body = app.post("/api/lifecycle/raise", map[string]any{
			"id": id, "event": "Release", "payload": nil,
		})
		if code != http.StatusAccepted {
			t.Fatalf("release gate: status=%d body=%s", code, body)
		}
		final := app.managementStatus("wait-completion", id)
		if final.RuntimeStatus != "Completed" || string(final.Output) != `"early event"` {
			t.Fatalf("unexpected early event result: %+v", final)
		}
	})

	t.Run("WaitingInstanceResumesAfterHostRestart", func(t *testing.T) {
		if app.restart == nil {
			t.Skip("restart requires a suite-owned local host, not DURABLE_E2E_BASE_URL")
		}
		driver := &durableApp{t: t, baseURL: app.baseURL}
		id := driver.scheduleOrchestration("/api/expenses", newExpense(t, autoApproveLimit+500))
		waiting := func(s orchestrationStatus) bool {
			return s.RuntimeStatus == "Running" && customStatusText(s.CustomStatus) == "awaiting manager approval"
		}
		driver.waitForStatus(id, "persisted approval wait", durableCompleteTimeout, waiting)
		app.baseURL = app.restart(t)
		driver.baseURL = app.baseURL
		driver.waitForStatus(id, "approval wait after restart", durableCompleteTimeout, waiting)
		driver.approve(id, decision{Approved: true, By: "after-restart"})
		final := driver.managementStatus("wait-completion", id)
		result := decodeExpenseResult(t, final.Output)
		if final.RuntimeStatus != "Completed" || result.Status != "approved" || result.Note != "decided by after-restart" {
			t.Fatalf("unexpected resumed result: %+v, status=%s", result, final.RuntimeStatus)
		}
	})
}
