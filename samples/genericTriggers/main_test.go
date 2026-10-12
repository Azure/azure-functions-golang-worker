package main

import (
	"encoding/json"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
)

func TestScenarios(t *testing.T) {
	for scenario, expected := range map[string]int{"queues": 4, "eventhub": 1, "mcp": 1} {
		t.Run(scenario, func(t *testing.T) {
			app, err := newApp(scenario)
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			app.GetRegisteredFunctions().Range(func(_, value any) bool {
				rf := value.(*sdk.RegisteredFunction)
				if _, err := json.Marshal(rf.RawBindings); err != nil {
					t.Error(err)
				}
				count++
				return true
			})
			if count != expected {
				t.Fatalf("functions=%d, want %d", count, expected)
			}
		})
	}
	if _, err := newApp("typo"); err == nil {
		t.Fatal("expected unknown scenario error")
	}
}

func TestEchoTool(t *testing.T) {
	var call ToolCall
	call.Arguments.Text = "hello"
	result, err := echoTool(t.Context(), call)
	if err != nil || result != "hello" {
		t.Fatalf("result=%q err=%v", result, err)
	}
}
