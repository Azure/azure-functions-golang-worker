// Package queue illustrates how a typed extension package can build on
// sdk.App.GenericTrigger. It is optional sample code, not a Storage SDK.
package queue

import (
	"context"
	"strings"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
)

// Config gives applications typed field names while the package owns the host
// binding schema. Connection is an app-setting name, not a connection string.
type Config struct {
	QueueName  string
	Connection string
}

// Register translates Config into generic metadata and preserves the original
// typed handler. T is inferred at the call site. No middleware, factory, global
// registration, or alternate dispatcher is needed for ordinary JSON payloads.
func Register[T any](app *sdk.App, name string, handler func(context.Context, T) error, cfg Config, opts ...sdk.Option) *sdk.RegisteredFunction {
	if strings.TrimSpace(cfg.QueueName) == "" || strings.TrimSpace(cfg.Connection) == "" {
		panic("queue.Register: QueueName and Connection are required")
	}
	return app.GenericTrigger(name, handler, &bindings.GenericTrigger{
		Type: "queueTrigger", Name: "message", DataType: "string",
		Properties: map[string]any{"queueName": cfg.QueueName, "connection": cfg.Connection},
	}, opts...)
}
