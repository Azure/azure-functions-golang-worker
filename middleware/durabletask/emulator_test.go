package durabletask

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// durabletask-go v1.0.0 removed its embedded backends (backend and
// backend/sqlite) and keeps its protobuf types internal, so the tests cannot
// stand up the engine the way they used to. This file provides a small
// in-memory stand-in instead.
//
// Messages are built from JSON and routed by name through the protobuf
// registry, which the task package populates when it is linked in. That keeps
// the tests free of the unexported types while still driving the real task
// executor, and the real management client against a real gRPC server.

const sidecarService = "TaskHubSidecarService"

// newProto returns a new, empty message of the named durabletask type.
func newProto(t testing.TB, name protoreflect.FullName) proto.Message {
	t.Helper()
	mt, err := protoregistry.GlobalTypes.FindMessageByName(name)
	if err != nil {
		t.Fatalf("find message type %q: %v", name, err)
	}
	return mt.New().Interface()
}

// marshalHistoryEvent builds a marshaled HistoryEvent from its JSON form.
func marshalHistoryEvent(t testing.TB, event map[string]any) []byte {
	t.Helper()
	if _, ok := event["timestamp"]; !ok {
		event["timestamp"] = time.Now().UTC().Format(time.RFC3339Nano)
	}
	j, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event json: %v", err)
	}
	msg := newProto(t, "HistoryEvent")
	if err := protojson.Unmarshal(j, msg); err != nil {
		t.Fatalf("build history event: %v", err)
	}
	raw, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal history event: %v", err)
	}
	return raw
}

func orchestratorStartedEvent(t testing.TB) []byte {
	return marshalHistoryEvent(t, map[string]any{"orchestratorStarted": map[string]any{}})
}

func executionStartedEvent(t testing.TB, instanceID, name, input string) []byte {
	return marshalHistoryEvent(t, map[string]any{
		"executionStarted": map[string]any{
			"name":                  name,
			"input":                 input,
			"orchestrationInstance": map[string]any{"instanceId": instanceID},
		},
	})
}

var instanceCounter struct {
	sync.Mutex
	n int
}

func nextInstanceID() string {
	instanceCounter.Lock()
	defer instanceCounter.Unlock()
	instanceCounter.n++
	return fmt.Sprintf("test-instance-%d", instanceCounter.n)
}

// scheduleAndEncodeRequest returns the base64 OrchestratorRequest the host
// would send for the first turn of a new orchestration: no past events, and an
// OrchestratorStarted plus ExecutionStarted pair as the new events.
func scheduleAndEncodeRequest(t testing.TB, name string, input any) string {
	t.Helper()
	return encodeFirstTurn(t, nextInstanceID(), name, input)
}

func encodeFirstTurn(t testing.TB, instanceID, name string, input any) string {
	t.Helper()
	j, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}
	raw := encodeOrchestratorRequest(instanceID, nil, [][]byte{
		orchestratorStartedEvent(t),
		executionStartedEvent(t, instanceID, name, string(j)),
	})
	return base64.StdEncoding.EncodeToString(raw)
}

// decodeRequest unwraps a base64-encoded OrchestratorRequest into the pieces
// the runner works with.
func decodeRequest(t testing.TB, encoded string) (string, [][]byte, [][]byte) {
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

// executeActivity runs an activity through the executor, naming the internal
// event type by inference the same way executeOrchestrator does.
func executeActivity[E any, PE interface {
	*E
	proto.Message
}](
	ctx context.Context,
	execute func(context.Context, api.InstanceID, PE) (PE, error),
	instanceID string,
	scheduled []byte,
) ([]byte, error) {
	ev := PE(new(E))
	if err := proto.Unmarshal(scheduled, ev); err != nil {
		return nil, err
	}
	result, err := execute(ctx, api.InstanceID(instanceID), ev)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(result)
}

type emulatedInstance struct {
	id           string
	name         string
	input        string
	status       string
	output       string
	customStatus string
	failure      any
	history      [][]byte
	pending      [][]byte
	running      bool
	created      time.Time
	updated      time.Time
	completed    time.Time
}

func (i *emulatedInstance) terminal() bool {
	switch i.status {
	case "ORCHESTRATION_STATUS_COMPLETED", "ORCHESTRATION_STATUS_FAILED",
		"ORCHESTRATION_STATUS_TERMINATED", "ORCHESTRATION_STATUS_CANCELED":
		return true
	}
	return false
}

// emulator is an in-memory durable task engine. It executes the registered
// orchestrators and activities with the real task executor and serves the
// management half of the TaskHubSidecarService protocol, which is what the
// Durable Task Scheduler and the Functions host expose to [Client].
//
// It supports sequential and fan-out activities, timers, external events and
// custom status, which is what the tests need. Anything else fails the test
// instead of being silently ignored.
type emulator struct {
	t        *testing.T
	executor task.Executor
	ctx      context.Context

	mu        sync.Mutex
	instances map[string]*emulatedInstance
}

func newEmulator(t *testing.T, r *task.TaskRegistry) *emulator {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &emulator{
		t:         t,
		executor:  task.NewTaskExecutor(r),
		ctx:       ctx,
		instances: map[string]*emulatedInstance{},
	}
}

func (e *emulator) start(name, instanceID, input string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if instanceID == "" {
		instanceID = nextInstanceID()
	}
	now := time.Now()
	inst := &emulatedInstance{
		id: instanceID, name: name, input: input,
		status: "ORCHESTRATION_STATUS_RUNNING", created: now, updated: now,
		pending: [][]byte{executionStartedEvent(e.t, instanceID, name, input)},
	}
	e.instances[instanceID] = inst
	e.kickLocked(inst)
	return instanceID
}

func (e *emulator) raise(instanceID, name, input string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	inst, ok := e.instances[instanceID]
	if !ok {
		return false
	}
	e.enqueueLocked(inst, marshalHistoryEvent(e.t, map[string]any{
		"eventRaised": map[string]any{"name": name, "input": input},
	}))
	return true
}

func (e *emulator) enqueueLocked(inst *emulatedInstance, event []byte) {
	if inst.terminal() {
		return
	}
	inst.pending = append(inst.pending, event)
	e.kickLocked(inst)
}

func (e *emulator) kickLocked(inst *emulatedInstance) {
	if inst.running || len(inst.pending) == 0 || inst.terminal() {
		return
	}
	inst.running = true
	go e.drain(inst)
}

// drain replays the orchestrator once per batch of pending events until none
// remain, applying the actions each turn produces.
func (e *emulator) drain(inst *emulatedInstance) {
	for {
		e.mu.Lock()
		if len(inst.pending) == 0 || inst.terminal() || e.ctx.Err() != nil {
			inst.running = false
			e.mu.Unlock()
			return
		}
		past := inst.history
		next := append([][]byte{orchestratorStartedEvent(e.t)}, inst.pending...)
		inst.pending = nil
		e.mu.Unlock()

		results, err := executeOrchestrator(e.ctx, e.executor.ExecuteOrchestrator, inst.id, past, next)

		e.mu.Lock()
		inst.history = append(append([][]byte(nil), past...), next...)
		inst.updated = time.Now()
		if err != nil {
			inst.status = "ORCHESTRATION_STATUS_FAILED"
			inst.failure = map[string]any{"errorType": "ExecutionError", "errorMessage": err.Error()}
			inst.completed = time.Now()
			inst.running = false
			e.mu.Unlock()
			return
		}
		inst.customStatus = results.Response.GetCustomStatus().GetValue()
		for _, a := range results.Response.GetActions() {
			pa := emulatedAction{id: a.GetId()}
			switch {
			case a.GetScheduleTask() != nil:
				st := a.GetScheduleTask()
				pa.kind, pa.name = "task", st.GetName()
				if st.GetInput() != nil {
					v := st.GetInput().GetValue()
					pa.input = &v
				}
			case a.GetCreateTimer() != nil:
				pa.kind, pa.fireAt = "timer", a.GetCreateTimer().GetFireAt().AsTime()
			case a.GetCompleteOrchestration() != nil:
				c := a.GetCompleteOrchestration()
				pa.kind = "complete"
				pa.status = c.GetOrchestrationStatus().String()
				pa.result = c.GetResult().GetValue()
				if fd := c.GetFailureDetails(); fd != nil {
					j, err := protojson.Marshal(fd)
					if err != nil {
						e.t.Errorf("marshal failure details: %v", err)
					} else if err := json.Unmarshal(j, &pa.failure); err != nil {
						e.t.Errorf("decode failure details: %v", err)
					}
				}
			}
			e.applyActionLocked(inst, pa)
		}
		e.mu.Unlock()
	}
}

type emulatedAction struct {
	id      int32
	kind    string
	name    string
	input   *string
	fireAt  time.Time
	status  string
	result  string
	failure any
}

func (e *emulator) applyActionLocked(inst *emulatedInstance, a emulatedAction) {
	switch a.kind {
	case "task":
		body := map[string]any{"name": a.name}
		if a.input != nil {
			body["input"] = *a.input
		}
		scheduled := marshalHistoryEvent(e.t, map[string]any{"eventId": a.id, "taskScheduled": body})
		inst.history = append(inst.history, scheduled)
		go func() {
			result, err := executeActivity(e.ctx, e.executor.ExecuteActivity, inst.id, scheduled)
			if e.ctx.Err() != nil {
				return
			}
			if err != nil {
				e.t.Errorf("execute activity %q: %v", a.name, err)
				return
			}
			e.mu.Lock()
			defer e.mu.Unlock()
			e.enqueueLocked(inst, result)
		}()

	case "timer":
		fireAt := a.fireAt.UTC().Format(time.RFC3339Nano)
		inst.history = append(inst.history, marshalHistoryEvent(e.t, map[string]any{
			"eventId":      a.id,
			"timerCreated": map[string]any{"fireAt": fireAt},
		}))
		time.AfterFunc(time.Until(a.fireAt), func() {
			if e.ctx.Err() != nil {
				return
			}
			e.mu.Lock()
			defer e.mu.Unlock()
			e.enqueueLocked(inst, marshalHistoryEvent(e.t, map[string]any{
				"timerFired": map[string]any{"timerId": a.id, "fireAt": fireAt},
			}))
		})

	case "complete":
		inst.status = a.status
		inst.output = a.result
		inst.failure = a.failure
		inst.completed = time.Now()
		inst.history = append(inst.history, marshalHistoryEvent(e.t, map[string]any{
			"eventId":            a.id,
			"executionCompleted": map[string]any{"orchestrationStatus": a.status, "result": a.result},
		}))

	default:
		e.t.Errorf("emulator does not support orchestrator action %d", a.id)
	}
}

func (e *emulator) stateJSON(inst *emulatedInstance) map[string]any {
	state := map[string]any{
		"instanceId":           inst.id,
		"name":                 inst.name,
		"orchestrationStatus":  inst.status,
		"input":                inst.input,
		"createdTimestamp":     inst.created.UTC().Format(time.RFC3339Nano),
		"lastUpdatedTimestamp": inst.updated.UTC().Format(time.RFC3339Nano),
	}
	if inst.output != "" {
		state["output"] = inst.output
	}
	if inst.customStatus != "" {
		state["customStatus"] = inst.customStatus
	}
	if inst.failure != nil {
		state["failureDetails"] = inst.failure
	}
	if !inst.completed.IsZero() {
		state["completedTimestamp"] = inst.completed.UTC().Format(time.RFC3339Nano)
	}
	return state
}

// lookup returns the GetInstanceResponse body for instanceID. With wait set it
// blocks until the instance reaches a terminal state.
func (e *emulator) lookup(ctx context.Context, instanceID string, wait bool) map[string]any {
	for {
		e.mu.Lock()
		inst, ok := e.instances[instanceID]
		var resp map[string]any
		done := true
		if !ok {
			resp = map[string]any{"exists": false}
		} else {
			resp = map[string]any{"exists": true, "orchestrationState": e.stateJSON(inst)}
			done = !wait || inst.terminal()
		}
		e.mu.Unlock()
		if done || ctx.Err() != nil {
			return resp
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// handle serves TaskHubSidecarService management calls. Requests and responses
// are converted through JSON so the handler never names the internal types.
func (e *emulator) handle(_ any, stream grpc.ServerStream) error {
	full, _ := grpc.MethodFromServerStream(stream)
	parts := strings.Split(strings.TrimPrefix(full, "/"), "/")
	if len(parts) != 2 || parts[0] != sidecarService {
		return status.Errorf(codes.Unimplemented, "unknown method %s", full)
	}
	desc, err := protoregistry.GlobalFiles.FindDescriptorByName(sidecarService)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	md := desc.(protoreflect.ServiceDescriptor).Methods().ByName(protoreflect.Name(parts[1]))
	if md == nil {
		return status.Errorf(codes.Unimplemented, "unknown method %s", full)
	}

	req := newProto(e.t, md.Input().FullName())
	if err := stream.RecvMsg(req); err != nil {
		return err
	}
	reqJSON, err := protojson.Marshal(req)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	var in map[string]any
	if err := json.Unmarshal(reqJSON, &in); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	str := func(key string) string { s, _ := in[key].(string); return s }

	out := map[string]any{}
	switch parts[1] {
	case "StartInstance":
		out["instanceId"] = e.start(str("name"), str("instanceId"), str("input"))
	case "GetInstance":
		out = e.lookup(stream.Context(), str("instanceId"), false)
	case "WaitForInstanceStart":
		out = e.lookup(stream.Context(), str("instanceId"), false)
	case "WaitForInstanceCompletion":
		out = e.lookup(stream.Context(), str("instanceId"), true)
	case "RaiseEvent":
		if !e.raise(str("instanceId"), str("name"), str("input")) {
			return status.Error(codes.NotFound, "instance not found")
		}
	default:
		return status.Errorf(codes.Unimplemented, "emulator does not implement %s", full)
	}

	respJSON, err := json.Marshal(out)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	resp := newProto(e.t, md.Output().FullName())
	if err := protojson.Unmarshal(respJSON, resp); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	return stream.SendMsg(resp)
}

func (e *emulator) serve(lis net.Listener) {
	server := grpc.NewServer(grpc.UnknownServiceHandler(e.handle))
	go func() {
		if err := server.Serve(lis); err != nil {
			e.t.Errorf("serve protocol fixture: %v", err)
		}
	}()
	e.t.Cleanup(server.Stop)
}

// startGrpcSidecar serves an emulator for the registry over an in-memory
// listener and returns a connection to it. It exercises the management
// [Client] against the subset of TaskHubSidecarService implemented here,
// not against the Functions host or Durable Task Scheduler.
func startGrpcSidecar(t *testing.T, r *task.TaskRegistry) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	newEmulator(t, r).serve(lis)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// startGrpcSidecarTCP is the TCP-listener variant of startGrpcSidecar. It
// returns the "host:port" address so a test can exercise the real dial path
// (grpc.NewClient against an address), which is what the durable client
// binding delivery uses.
func startGrpcSidecarTCP(t *testing.T, r *task.TaskRegistry) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	newEmulator(t, r).serve(lis)
	return lis.Addr().String()
}
