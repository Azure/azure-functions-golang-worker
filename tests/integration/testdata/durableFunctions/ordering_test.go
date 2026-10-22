package main

import (
	"encoding/base64"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func TestCompletedInvocations_ExcludesInFlightObservers(t *testing.T) {
	inFlight := &invocationProbe{
		Name: "Blocked", Error: "blocked by ordering probe",
		Steps: []string{"A:before", "B:before", "B:after"},
	}
	completed := &invocationProbe{Name: "InputProbe", Complete: true}
	records := map[string]*invocationProbe{"in-flight": inFlight, "complete": completed}
	got := completedInvocations(records)
	if len(got) != 1 || got["complete"] != completed {
		t.Fatalf("snapshot published an incomplete invocation: %+v", got)
	}
	inFlight.Steps = append(inFlight.Steps, "A:after")
	if _, ok := completedInvocations(records)["in-flight"]; ok {
		t.Fatal("observer completion alone must not precede outer OTel completion")
	}
	inFlight.Complete = true
	if got := completedInvocations(records); len(got) != 2 || got["in-flight"] != inFlight {
		t.Fatalf("completed invocation is missing: %+v", got)
	}
}

func TestReplaceProbeInput_PreservesUpstreamRequest(t *testing.T) {
	messageType, err := protoregistry.GlobalTypes.FindMessageByName("OrchestratorRequest")
	if err != nil {
		t.Fatal(err)
	}
	build := func(input string) proto.Message {
		t.Helper()
		message := messageType.New().Interface()
		payload := `{
				"instanceId": "probe-instance",
				"pastEvents": [{"eventId": 9, "orchestratorStarted": {}}],
				"newEvents": [
					{"timestamp": "2026-10-22T12:00:00Z", "orchestratorStarted": {}},
					{"eventId": 2, "executionStarted": {
						"name": "InputProbe", "input": "` + input + `",
						"orchestrationInstance": {"instanceId": "probe-instance"}
					}}
				]
			}`
		if err := protojson.Unmarshal([]byte(payload), message); err != nil {
			t.Fatal(err)
		}
		message.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 100, protowire.VarintType), 42))
		return message
	}
	raw, err := proto.Marshal(build("0"))
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.StdEncoding.EncodeToString(raw)
	got, err := replaceProbeInput(encoded, "0", "42")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.StdEncoding.DecodeString(got)
	if err != nil {
		t.Fatal(err)
	}
	actual := messageType.New().Interface()
	if err := proto.Unmarshal(decoded, actual); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(actual, build("42")) {
		t.Fatalf("rewrite changed fields other than the new execution input: %v", actual)
	}
	if _, err := replaceProbeInput(encoded, "unexpected", "42"); err == nil {
		t.Fatal("mismatched input must fail")
	}
}

func TestReplaceProbeInput_RejectsInvalidRequests(t *testing.T) {
	for _, encoded := range []string{
		"not base64!",
		base64.StdEncoding.EncodeToString([]byte{0x22, 0x05}),
		base64.StdEncoding.EncodeToString([]byte{0x22, 0x01, 0xff}),
		base64.StdEncoding.EncodeToString(nil),
	} {
		if response, err := replaceProbeInput(encoded, "0", "42"); err == nil || response != "" {
			t.Fatalf("request %q: response=%q err=%v, want failure", encoded, response, err)
		}
	}
}
