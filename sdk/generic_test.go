package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
)

type recursiveGenericPointer *recursiveGenericPointer

// scoreTable has a key type JSON cannot use directly, so its custom codecs
// must keep it valid as both a payload and a result.
type scoreTable map[float64]string

func (s *scoreTable) UnmarshalJSON(data []byte) error {
	var values map[string]string
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}
	*s = scoreTable{}
	for key, value := range values {
		score, err := strconv.ParseFloat(key, 64)
		if err != nil {
			return err
		}
		(*s)[score] = value
	}
	return nil
}

func (s scoreTable) MarshalJSON() ([]byte, error) {
	values := make(map[string]string, len(s))
	for score, value := range s {
		values[strconv.FormatFloat(score, 'g', -1, 64)] = value
	}
	return json.Marshal(values)
}

func TestGenericTriggerRegistration(t *testing.T) {
	for _, lowLevel := range []bool{false, true} {
		t.Run(fmt.Sprint(lowLevel), func(t *testing.T) {
			app := FunctionApp()
			props := map[string]any{"nested": map[string]any{"enabled": false}, "number": int64(9007199254740993)}
			trigger := &bindings.GenericTrigger{Type: "acmeTrigger", Name: "event", Properties: props}
			handler := func(context.Context, []byte) error { return nil }
			var rf *RegisteredFunction
			if lowLevel {
				rf = app.RegisterFunction("Orders", handler, trigger)
			} else {
				rf = app.GenericTrigger("Orders", handler, trigger)
			}
			props["nested"].(map[string]any)["enabled"] = true
			props["number"] = 0
			trigger.Type = "changedTrigger"
			data, err := json.Marshal(rf.RawBindings[0])
			if err != nil {
				t.Fatal(err)
			}
			if rf.TriggerType != "acmeTrigger" || !strings.Contains(string(data), `"enabled":false`) || !strings.Contains(string(data), "9007199254740993") {
				t.Fatalf("registration is not a snapshot: %s", data)
			}
			if rf.FuncId == "" || len(rf.RawBindings) != 1 {
				t.Fatalf("invalid registration: %+v", rf)
			}
		})
	}
}

func TestGenericTriggerRejectsInvalidRegistration(t *testing.T) {
	valid := func(context.Context, []byte) error { return nil }
	var nilHandler func(context.Context, []byte) error
	tests := []struct {
		name    string
		fn      any
		trigger *bindings.GenericTrigger
		opts    []Option
		want    string
	}{
		{name: "nil descriptor", fn: valid, want: "trigger"},
		{name: "empty type", fn: valid, trigger: &bindings.GenericTrigger{Name: "event"}, want: "type"},
		{name: "not trigger", fn: valid, trigger: &bindings.GenericTrigger{Type: "acme", Name: "event"}, want: "Trigger"},
		{name: "empty binding name", fn: valid, trigger: &bindings.GenericTrigger{Type: "acmeTrigger"}, want: "name"},
		{name: "http", fn: valid, trigger: &bindings.GenericTrigger{Type: "httpTrigger", Name: "event"}, want: "HTTP"},
		{name: "data type", fn: valid, trigger: &bindings.GenericTrigger{Type: "acmeTrigger", Name: "event", DataType: "stream"}, want: "dataType"},
		{name: "cardinality", fn: valid, trigger: &bindings.GenericTrigger{Type: "acmeTrigger", Name: "event", Cardinality: "all"}, want: "cardinality"},
		{name: "scalar batch", fn: valid, trigger: &bindings.GenericTrigger{Type: "acmeTrigger", Name: "event", Cardinality: "many"}, want: "slice"},
		{name: "not function", fn: 42, want: "handler"},
		{name: "nil function", fn: nilHandler, want: "handler"},
		{name: "wrong context", fn: func(string, []byte) error { return nil }, want: "context.Context"},
		{name: "context payload", fn: func(context.Context, context.Context) error { return nil }, want: "payload"},
		{name: "extra argument", fn: func(context.Context, []byte, string) error { return nil }, want: "2 arguments"},
		{name: "no error", fn: func(context.Context, []byte) {}, want: "error"},
		{name: "too many results", fn: func(context.Context, []byte) (string, int, error) { return "", 0, nil }, want: "error"},
		{name: "channel payload", fn: func(context.Context, chan int) error { return nil }, want: "payload"},
		{name: "interface payload", fn: func(context.Context, io.Reader) error { return nil }, want: "payload type io.Reader"},
		{name: "float map key payload", fn: func(context.Context, map[float64]string) error { return nil }, want: "map key float64"},
		{name: "channel result", fn: func(context.Context, []byte) (chan int, error) { return nil, nil }, want: "result type chan int"},
		{name: "function batch result", fn: func(context.Context, []byte) ([]func(), error) { return nil, nil }, want: "result type []func()"},
		{name: "interface map key result", fn: func(context.Context, []byte) (map[any]string, error) { return nil, nil }, want: "map key interface {}"},
		{name: "recursive pointer", fn: func(context.Context, recursiveGenericPointer) error { return nil }, want: "recursive"},
		{name: "recursive batch element", fn: func(context.Context, []recursiveGenericPointer) error { return nil }, want: "recursive"},
		{name: "recursive map value", fn: func(context.Context, map[string]recursiveGenericPointer) error { return nil }, want: "recursive"},
		{name: "recursive struct field", fn: func(context.Context, struct{ Value recursiveGenericPointer }) error { return nil }, want: "recursive"},
		{name: "post option invalid", fn: valid, opts: []Option{func(rf *RegisteredFunction) { rf.RawBindings[0].GenericBinding.Properties["type"] = "override" }}, want: "reserved"},
		{name: "named output", fn: valid, opts: []Option{func(rf *RegisteredFunction) {
			rf.RawBindings = append(rf.RawBindings, bindings.Binding{Name: "other", Type: "queue", Direction: "out"})
		}}, want: "$return"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trigger := tt.trigger
			if trigger == nil && tt.name != "nil descriptor" {
				trigger = &bindings.GenericTrigger{Type: "acmeTrigger", Name: "event", Properties: map[string]any{}}
			}
			app := FunctionApp()
			defer func() {
				failure := recover()
				if failure == nil || !strings.Contains(fmt.Sprint(failure), tt.want) {
					t.Errorf("panic = %v, want containing %q", failure, tt.want)
				}
				app.GetRegisteredFunctions().Range(func(_, _ any) bool {
					t.Error("invalid function was stored")
					return false
				})
			}()
			app.GenericTrigger("Invalid", tt.fn, trigger, tt.opts...)
		})
	}
}

type countingGenericProperty struct{ calls *int }

func (p countingGenericProperty) MarshalJSON() ([]byte, error) {
	*p.calls++
	return []byte(`{"id":9007199254740993}`), nil
}

func TestGenericRegistrationMarshalsPropertiesOnce(t *testing.T) {
	calls := 0
	app := FunctionApp()
	rf := app.GenericTrigger("Once", func(context.Context, []byte) error { return nil }, &bindings.GenericTrigger{
		Type: "customTrigger", Name: "message", Properties: map[string]any{"nested": countingGenericProperty{&calls}},
	})
	if calls != 1 {
		t.Fatalf("property marshaled %d times during registration, want 1", calls)
	}
	data, err := json.Marshal(rf.RawBindings[0])
	if err != nil || !strings.Contains(string(data), `"nested":{"id":9007199254740993}`) || calls != 1 {
		t.Fatalf("snapshot=%s error=%v calls=%d", data, err, calls)
	}
}

func TestGenericRegistrationAllowsConvertibleTypes(t *testing.T) {
	for name, fn := range map[string]any{
		"custom codecs":   func(context.Context, scoreTable) (scoreTable, error) { return nil, nil },
		"pointer results": func(context.Context, *string) (*[]byte, error) { return nil, nil },
		"dynamic result":  func(context.Context, []any) (any, error) { return nil, nil },
		"arrays":          func(context.Context, [2]int) ([2]string, error) { return [2]string{}, nil },
		// Struct fields follow encoding/json rules and are checked during conversion.
		"struct fields": func(context.Context, struct{ Body io.Reader }) (struct{ Done chan int }, error) {
			return struct{ Done chan int }{}, nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			FunctionApp().GenericTrigger("Valid", fn, &bindings.GenericTrigger{Type: "customTrigger", Name: "event"})
		})
	}
}

func TestGenericRegistrationAllowsRecursiveModels(t *testing.T) {
	type Node struct {
		Next     *Node   `json:"next"`
		Children []*Node `json:"children"`
	}
	FunctionApp().GenericTrigger("Nodes", func(context.Context, []Node) error { return nil }, &bindings.GenericTrigger{
		Type: "customTrigger", Name: "nodes", Cardinality: "many",
	})
}
