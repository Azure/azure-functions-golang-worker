package integration

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"
)

// Runs the real registered functions through Core Tools, including FunctionRpc
// argument/return binding and HTTP streaming. No synthetic next handler is used.
func TestDurableMiddlewareOrdering(t *testing.T) {
	for _, order := range []string{"before", "after"} {
		t.Run(order, func(t *testing.T) {
			host, _ := startDurableHost(t, map[string]string{
				"DURABLE_TEST_MIDDLEWARE_ORDER":                 order,
				"AZURE_FUNCTIONS_WORKER_OPENTELEMETRY_DISABLED": "false",
			})
			app := &durableApp{t: t, baseURL: host.URL()}
			inputID := app.scheduleOrchestration("/api/ordering/start/InputProbe", nil)
			inputStatus := app.waitForCompletion(inputID)
			if string(inputStatus.Output) != "42" {
				t.Errorf("replay used output %s, want 42 from the final middleware replacement (bound input was 0)", inputStatus.Output)
			}
			id := app.scheduleOrchestration("/api/ordering/start/Parent", nil)
			st := app.waitForStatus(id, "parent completion", 3*time.Minute, func(s orchestrationStatus) bool {
				return s.RuntimeStatus == "Completed"
			})
			if string(st.Output) != "3" {
				t.Fatalf("parent output = %s, want 3", st.Output)
			}
			// The child uses an auto-generated ID and continues as new twice.
			child := app.status(id + ":0000")
			if child.RuntimeStatus != "Completed" || string(child.Output) != "3" {
				t.Fatalf("unexpected child status: %+v", child)
			}
			failed := app.scheduleOrchestration("/api/ordering/start/Failing", nil)
			app.waitForStatus(failed, "orchestration failure", durableCompleteTimeout, func(s orchestrationStatus) bool {
				return s.RuntimeStatus == "Failed"
			})
			panicked := app.scheduleOrchestration("/api/ordering/start/Panicking", nil)
			app.waitForStatus(panicked, "orchestration panic", durableCompleteTimeout, func(s orchestrationStatus) bool {
				return s.RuntimeStatus == "Failed"
			})
			app.scheduleOrchestration("/api/ordering/start/Blocked", nil)

			var snapshot struct {
				BlockedCalls int32
				Invocations  map[string]struct {
					Name       string
					Steps      []string
					ReturnSet  bool
					ClientSeen bool
					Error      string
					InputSeen  string
				}
				Spans []struct {
					InvocationID string
					InstanceID   string
					TraceID      string
					ParentID     string
					Status       string
				}
			}
			// Wait for the rejected invocation to finish unwinding its observers.
			// A host may retry it, so check the behavior, not an exact attempt count.
			deadline := time.Now().Add(durableCompleteTimeout)
			client := &http.Client{Timeout: 10 * time.Second}
			for {
				resp, err := client.Get(host.URL() + "/api/ordering/snapshot")
				if err != nil {
					t.Fatal(err)
				}
				err = json.NewDecoder(resp.Body).Decode(&snapshot)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK || err != nil {
					t.Fatalf("snapshot status=%d error=%v", resp.StatusCode, err)
				}
				seen := false
				for _, record := range snapshot.Invocations {
					seen = seen || (record.Name == "Blocked" && record.Error == "blocked by ordering probe")
				}
				if seen || snapshot.BlockedCalls > 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("blocking middleware did not observe the orchestration")
				}
				time.Sleep(durablePollInterval)
			}
			if snapshot.BlockedCalls != 0 {
				t.Errorf("blocked orchestrator ran %d times", snapshot.BlockedCalls)
			}
			counts := map[string]int{}
			for invocationID, record := range snapshot.Invocations {
				if !reflect.DeepEqual(record.Steps, []string{"A:before", "B:before", "B:after", "A:after"}) {
					t.Errorf("%s %s steps = %v", record.Name, invocationID, record.Steps)
				}
				counts[record.Name]++
				if record.Name == "Blocked" {
					if record.ReturnSet || record.Error != "blocked by ordering probe" {
						t.Errorf("unexpected rejected invocation: %+v", record)
					}
					continue
				}
				if record.Name == "InputProbe" && record.InputSeen != "42" {
					t.Errorf("input transformation did not run: %+v", record)
				}
				if record.Name != "InputProbe" && record.Name != "Parent" && record.Name != "CounterOrchestration" && record.Name != "Failing" && record.Name != "Panicking" {
					continue
				}
				if record.ClientSeen || !record.ReturnSet || record.Error != "" {
					t.Errorf("replay outcome/client isolation: %+v", record)
				}
				// beta3 encodes both errors and orchestrator panics as failed
				// orchestration responses, not middleware invocation errors.
				found := 0
				for _, span := range snapshot.Spans {
					if span.InvocationID == invocationID {
						found++
						if span.InstanceID == "" || span.TraceID != "11111111111111111111111111111111" || span.ParentID != "2222222222222222" {
							t.Errorf("missing replay annotations or injected parent context: %+v", span)
						}
						if span.Status == "Error" {
							t.Errorf("successfully processed replay was marked as an invocation error: %+v", span)
						}
					}
				}
				if found != 1 {
					t.Errorf("%s %s has %d invocation spans, want 1", record.Name, invocationID, found)
				}
			}
			for name, minimum := range map[string]int{"InputProbe": 1, "Parent": 2, "CounterOrchestration": 6, "ProbeActivity": 3, "Failing": 1, "Panicking": 1, "Blocked": 1, "OrderingStart": 5} {
				if counts[name] < minimum {
					t.Errorf("observed %d %s invocations, want at least %d", counts[name], name, minimum)
				}
			}
		})
	}
}
