package integration

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// This test starts a dedicated task hub and does not delete or clean shared
// Azurite containers. Entity executions cover host dispatch, state, signals,
// calls, locks, timer replay, and operation failure propagation.
func TestDurableEntities(t *testing.T) {
	app := startDurableEntityApp(t)

	t.Run("SignalsCallsAndLocks", func(t *testing.T) {
		subApp := *app
		subApp.t = t
		instanceID := subApp.scheduleOrchestration("/api/entities", nil)
		status := subApp.waitForCompletion(instanceID)
		var count int
		if err := json.Unmarshal(status.Output, &count); err != nil {
			t.Fatalf("decode entity count %s: %v", status.Output, err)
		}
		if count != 5 {
			t.Fatalf("entity count = %d, want 5 after immediate and scheduled signals", count)
		}
	})

	t.Run("OperationFailureReturnedByCall", func(t *testing.T) {
		subApp := *app
		subApp.t = t
		instanceID := subApp.scheduleOrchestration("/api/entity-failure", nil)
		status := subApp.waitForCompletion(instanceID)
		var message string
		if err := json.Unmarshal(status.Output, &message); err != nil {
			t.Fatalf("decode entity call failure %s: %v", status.Output, err)
		}
		if !strings.Contains(message, "intentional entity failure") {
			t.Fatalf("entity call error = %q, want the operation failure", message)
		}
	})

	t.Run("ManagementSignalAndQuery", func(t *testing.T) {
		subApp := *app
		subApp.t = t
		status, body := subApp.post("/api/entity-management", nil)
		if status != 200 {
			t.Fatalf("entity management: expected status 200, got %d: %s", status, body)
		}
		var result struct {
			State      int `json:"state"`
			QueryState int `json:"queryState"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatalf("decode entity management response %s: %v", body, err)
		}
		if result.State != 7 || result.QueryState != 7 {
			t.Fatalf("entity management state=%d queryState=%d, want both 7", result.State, result.QueryState)
		}
	})
}

func startDurableEntityApp(t *testing.T) *durableApp {
	t.Helper()
	if baseURL := os.Getenv(durableBaseURLEnv); baseURL != "" {
		return &durableApp{t: t, baseURL: strings.TrimSuffix(baseURL, "/")}
	}

	host, _ := startDurableHost(t, nil)
	return &durableApp{t: t, baseURL: host.URL()}
}
