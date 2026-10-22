package durabletask

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// A first turn carries ExecutionStarted in newEvents and no past events; every
// later turn carries past events. Those are the only shapes the host produces,
// verified against captured requests from sequential, fan-out, timer,
// external-event, ContinueAsNew and sub-orchestration runs.
func TestCheckHistoryPresent_AcceptsFirstAndLaterTurns(t *testing.T) {
	encoded := scheduleAndEncodeRequest(t, "HelloCities", "")

	instanceID, pastEvents, newEvents := decodeRequest(t, encoded)

	if len(pastEvents) != 0 {
		t.Fatalf("expected a first turn with no past events, got %d", len(pastEvents))
	}
	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("HelloCities", helloCities); err != nil {
		t.Fatal(err)
	}
	runner := &orchestrationRunner{registry: registry}
	if _, err := runner.loadAndRun(context.Background(), encoded); err != nil {
		t.Errorf("first turn rejected: %v", err)
	}

	// A later turn has past events and no ExecutionStarted among the new ones.
	later := base64.StdEncoding.EncodeToString(encodeOrchestratorRequest(instanceID, newEvents, nil))
	if _, err := runner.loadAndRun(context.Background(), later); err != nil {
		t.Errorf("later turn rejected: %v", err)
	}
}

// Empty past events with no ExecutionStarted means the host withheld the
// history, which only happens under extended sessions. Replaying that would
// re-schedule completed work, so it must fail rather than proceed.
func TestCheckHistoryPresent_RejectsWithheldHistory(t *testing.T) {
	encoded := scheduleAndEncodeRequest(t, "HelloCities", "")

	_, _, newEvents := decodeRequest(t, encoded)

	// Drop the ExecutionStarted event to simulate a host that withheld history.
	withoutStart := newEvents[:1]
	runner := &orchestrationRunner{registry: task.NewTaskRegistry()}
	_, err := runner.loadAndRun(context.Background(),
		base64.StdEncoding.EncodeToString(encodeOrchestratorRequest("id-1", nil, withoutStart)))
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
	assignSubOrchestrationIDs("id-1", &task.ExecutionResults{})
}

// executeFirstTurn runs the named orchestrator's first turn and returns the
// engine's results, which carry the actions the orchestrator scheduled.
func executeFirstTurn(t *testing.T, registry *task.TaskRegistry, instanceID, name string) *task.ExecutionResults {
	t.Helper()

	encoded := scheduleAndEncodeRequest(t, name, "")
	_, pastEvents, newEvents := decodeRequest(t, encoded)

	executor := task.NewTaskExecutor(registry)
	results, err := executeOrchestrator(context.Background(), executor.ExecuteOrchestrator, instanceID, pastEvents, newEvents)
	if err != nil {
		t.Fatalf("execute orchestrator: %v", err)
	}
	return results
}

// Decode with the upstream request descriptor, independently of the runner's
// envelope parser, history conversion, and history-presence guard.
func executeUpstreamRequest[E, EP any, PE interface {
	*E
	proto.Message
}](t *testing.T, execute func(context.Context, api.InstanceID, []PE, []PE, *EP) (*task.ExecutionResults, error), encoded string) (*task.ExecutionResults, error) {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	request := newProto(t, "OrchestratorRequest")
	if err := proto.Unmarshal(raw, request); err != nil {
		t.Fatal(err)
	}
	message := request.ProtoReflect()
	fields := message.Descriptor().Fields()
	events := func(name protoreflect.Name) []PE {
		list := message.Get(fields.ByName(name)).List()
		out := make([]PE, list.Len())
		for i := range out {
			out[i] = list.Get(i).Message().Interface().(PE)
		}
		return out
	}
	return execute(context.Background(), api.InstanceID(message.Get(fields.ByName("instanceId")).String()),
		events("pastEvents"), events("newEvents"), nil)
}

func TestLoadAndRun_ReplayMatchesUpstream(t *testing.T) {
	d := Middleware(WithOrchestrator("HelloCities", helloCities))
	id := "replay-instance"
	past := [][]byte{
		orchestratorStartedEvent(t),
		executionStartedEvent(t, id, "HelloCities", `""`),
		marshalHistoryEvent(t, map[string]any{
			"eventId": 0, "taskScheduled": map[string]any{"name": "SayHello", "input": `"Tokyo"`},
		}),
	}
	next := [][]byte{
		orchestratorStartedEvent(t),
		marshalHistoryEvent(t, map[string]any{
			"taskCompleted": map[string]any{"taskScheduledId": 0, "result": `"Hello, Tokyo!"`},
		}),
	}
	raw := encodeOrchestratorRequest(id, past, next)
	raw = protowire.AppendTag(raw, 100, protowire.BytesType)
	raw = protowire.AppendString(raw, "unknown envelope field")
	encoded := base64.StdEncoding.EncodeToString(raw)
	got, err := d.runner.loadAndRun(context.Background(), encoded)
	if err != nil {
		t.Fatal(err)
	}
	direct, err := executeUpstreamRequest(t, task.NewTaskExecutor(d.registry).ExecuteOrchestrator, encoded)
	if err != nil {
		t.Fatal(err)
	}
	response := newProto(t, "OrchestratorResponse")
	responseBytes, err := base64.StdEncoding.DecodeString(got)
	if err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(responseBytes, response); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(response, direct.Response) {
		t.Fatal("replay response differs from upstream descriptor-based execution")
	}
	actions := direct.Response.GetActions()
	if len(actions) != 1 || actions[0].GetScheduleTask().GetInput().GetValue() != `"Seattle"` {
		t.Fatalf("replay must schedule only the next city, got %v", actions)
	}
}

func TestLoadAndRun_RejectsInvalidHistory(t *testing.T) {
	start := executionStartedEvent(t, "invalid-history", "Echo", `""`)
	overwritten := append([]byte(nil), start...)
	overwritten = protowire.AppendTag(overwritten, 7, protowire.BytesType) // taskCompleted replaces executionStarted.
	overwritten = protowire.AppendBytes(overwritten, nil)
	for _, tc := range []struct {
		name       string
		past, next [][]byte
		want       string
	}{
		{"malformed past event", [][]byte{{0xff}}, [][]byte{start}, "unmarshal history event"},
		{"malformed new event", nil, [][]byte{start, {0xff}}, "unmarshal history event"},
		{"truncated execution start", nil, [][]byte{{0x1a, 0x05}}, "unmarshal history event"},
		{"overwritten execution start", nil, [][]byte{overwritten}, "without history"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			d := Middleware(WithOrchestrator("Echo", func(*task.OrchestrationContext) (any, error) {
				calls++
				return nil, nil
			}))
			encoded := base64.StdEncoding.EncodeToString(encodeOrchestratorRequest("invalid-history", tc.past, tc.next))
			response, err := d.runner.loadAndRun(context.Background(), encoded)
			if err == nil || !strings.Contains(err.Error(), tc.want) || response != "" || calls != 0 {
				t.Fatalf("response=%q err=%v calls=%d, want %q before execution", response, err, calls, tc.want)
			}
		})
	}
}

func TestLoadAndRun_OrchestratorPanicIsFailureResponse(t *testing.T) {
	d := Middleware(WithOrchestrator("Panicking", func(*task.OrchestrationContext) (any, error) {
		panic("expected orchestrator panic")
	}))
	encoded := scheduleAndEncodeRequest(t, "Panicking", nil)
	output, err := d.runner.loadAndRun(context.Background(), encoded)
	if err != nil {
		t.Fatalf("panic must be encoded in the replay response, not an invocation error: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(output)
	if err != nil {
		t.Fatal(err)
	}
	results, err := executeUpstreamRequest(t, task.NewTaskExecutor(d.registry).ExecuteOrchestrator, encoded)
	if err != nil {
		t.Fatal(err)
	}
	proto.Reset(results.Response)
	if err := proto.Unmarshal(raw, results.Response); err != nil {
		t.Fatal(err)
	}
	actions := results.Response.GetActions()
	if len(actions) != 1 {
		t.Fatalf("expected one failed completion, got %v", actions)
	}
	completion := actions[0].GetCompleteOrchestration()
	if completion.GetOrchestrationStatus().String() != "ORCHESTRATION_STATUS_FAILED" {
		t.Fatalf("expected failed completion, got %v", completion)
	}
	failure := completion.GetFailureDetails()
	if failure.GetErrorType() != string(api.ErrorTypeOrchestratorPanic) || !strings.Contains(failure.GetErrorMessage(), "expected orchestrator panic") {
		t.Fatalf("panic details missing: %v", failure)
	}
}
