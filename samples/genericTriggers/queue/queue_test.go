package queue

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
)

func TestRegisterMatchesDirectGenericTrigger(t *testing.T) {
	handler := func(context.Context, map[string]string) error { return nil }
	cfg := Config{QueueName: "orders", Connection: "OrdersConnection"}
	app := sdk.FunctionApp()
	wrapped := Register(app, "Wrapped", handler, cfg)
	direct := app.GenericTrigger("Direct", handler, &bindings.GenericTrigger{
		Type: "queueTrigger", Name: "message", DataType: "string",
		Properties: map[string]any{"queueName": "orders", "connection": "OrdersConnection"},
	})
	w, err := json.Marshal(wrapped.RawBindings)
	if err != nil {
		t.Fatal(err)
	}
	d, err := json.Marshal(direct.RawBindings)
	if err != nil {
		t.Fatal(err)
	}
	if string(w) != string(d) {
		t.Fatalf("wrapper metadata=%s direct=%s", w, d)
	}
	if reflect.ValueOf(wrapped.Func).Pointer() != reflect.ValueOf(handler).Pointer() {
		t.Fatal("wrapper replaced the user handler")
	}
}

func TestRegisterValidatesConfig(t *testing.T) {
	for _, cfg := range []Config{{}, {QueueName: "orders"}, {Connection: "OrdersConnection"}} {
		t.Run(cfg.QueueName+cfg.Connection, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected invalid configuration panic")
				}
			}()
			Register(sdk.FunctionApp(), "Invalid", func(context.Context, string) error { return nil }, cfg)
		})
	}
}
