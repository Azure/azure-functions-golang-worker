package bindings

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestGenericTriggerMetadata(t *testing.T) {
	b := (&GenericTrigger{
		Type: "acmeTrigger", Name: "event", DataType: "binary", Cardinality: "many",
		Properties: map[string]any{
			"queueName": "%OrdersQueue%", "path": "orders/{id}",
			"enabled": false, "limit": 0, "empty": "",
			"nested": map[string]any{"items": []any{"one", 2}},
		},
	}).ToBinding()
	data, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"type": `"acmeTrigger"`, "name": `"event"`, "direction": `"in"`,
		"dataType": `"binary"`, "cardinality": `"many"`,
		"queueName": `"%OrdersQueue%"`, "path": `"orders/{id}"`,
		"enabled": "false", "limit": "0", "empty": `""`,
		"nested": `{"items":["one",2]}`,
	}
	if len(got) != len(want) {
		t.Fatalf("metadata = %s", data)
	}
	for key, expected := range want {
		if string(got[key]) != expected {
			t.Errorf("%s = %s, want %s", key, got[key], expected)
		}
	}
}

func TestGenericBindingRejectsConflictingProperties(t *testing.T) {
	for _, key := range []string{"name", "type", "direction", "dataType", "cardinality"} {
		for _, spelling := range []string{key, strings.ToUpper(key)} {
			t.Run(spelling, func(t *testing.T) {
				b := (&GenericTrigger{Type: "acmeTrigger", Name: "event", Properties: map[string]any{spelling: "override"}}).ToBinding()
				if _, err := json.Marshal(b); err == nil {
					t.Fatal("expected reserved property error")
				}
			})
		}
	}
	for _, props := range []map[string]any{
		{"queueName": "one", "QueueName": "two"},
		{"invalid": make(chan int)},
	} {
		b := (&GenericTrigger{Type: "acmeTrigger", Name: "event", Properties: props}).ToBinding()
		if _, err := json.Marshal(b); err == nil {
			t.Fatal("expected invalid properties error")
		}
	}
}

func TestGenericBindingRejectsEveryTypedConfiguration(t *testing.T) {
	bt := reflect.TypeFor[Binding]()
	for i := 0; i < bt.NumField(); i++ {
		field := bt.Field(i)
		if !field.Anonymous || field.Type.Kind() != reflect.Ptr {
			continue
		}
		t.Run(field.Name, func(t *testing.T) {
			b := (&GenericTrigger{Type: "acmeTrigger", Name: "event"}).ToBinding()
			reflect.ValueOf(&b).Elem().Field(i).Set(reflect.New(field.Type.Elem()))
			if _, err := json.Marshal(b); err == nil {
				t.Fatalf("generic configuration silently overrides %s", field.Name)
			}
		})
	}
}
