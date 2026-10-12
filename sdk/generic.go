package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"strings"

	"github.com/azure/azure-functions-golang-worker/internal/bindingtype"
	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
)

// GenericTrigger registers a host-provided non-HTTP trigger without a typed
// trigger package. The handler must be func(context.Context, T) error or
// func(context.Context, T) (R, error), where T is raw text, raw bytes, or
// JSON-decodable, and R is raw text, raw bytes, or JSON-encodable. It runs
// through the normal middleware chain. Registration panics on invalid metadata
// or handler signatures, like the other App registration methods. It also
// rejects types that JSON cannot represent, such as channels, functions,
// non-empty interface payloads, and map keys JSON cannot use. Custom JSON and
// text codecs are allowed. Struct fields are checked by encoding/json during
// conversion. Pointer-only recursive types such as type P *P are rejected
// anywhere in a payload, because decoding one can loop forever. Caller-owned
// properties are snapshotted.
//
// Strings and byte slices receive the host payload unchanged, including JSON
// quoting; their custom UnmarshalJSON methods are not called. json.Number is a
// numeric type, not raw text. Other types use JSON decoding with UseNumber, so
// dynamic fields (any and map[string]any) retain exact numbers as json.Number.
// Custom JSON decoders own their validation and numeric conversions.
//
// A named non-byte slice's UnmarshalJSON receives a JSON array for both JSON
// payloads and host collections. In the latter, raw text elements are quoted,
// raw bytes are base64 JSON strings, and structured elements stay JSON. The
// custom batch decoder owns null handling and element validation. Matching raw
// string/byte representations are not copied; treat incoming bytes as read-only.
// Host collections bind to slices;
// Cardinality "many" must be explicitly requested when the extension supports
// batching. SDK clients, HTTP, and trigger-specific execution protocols require
// their existing adapters. This method does not select a ClientFactory.
//
// A return value is only useful when consumed by the host trigger extension or
// a declared $return output binding. A nil pointer or interface result sends
// no value. Otherwise custom JSON and text marshalers apply first, then
// pointers are followed, so *string and *[]byte are sent like string and
// []byte, and other values are JSON. Text and JSON results must be valid UTF-8.
// Named output bindings are not supported.
// Trigger-specific options such as WithQueueName do not apply;
// put their host configuration in trigger.Properties instead.
func (app *App) GenericTrigger(name string, f any, trigger *bindings.GenericTrigger, opts ...Option) *RegisteredFunction {
	return app.RegisterFunction(name, f, trigger, opts...)
}

var genericBindingName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`)

func validateGenericHandler(f any, cardinality string) {
	fail := func(message string) { panic("GenericTrigger: " + message) }
	ft := reflect.TypeOf(f)
	if ft == nil || ft.Kind() != reflect.Func || reflect.ValueOf(f).IsNil() {
		fail("handler must be a non-nil function")
	}
	if ft.IsVariadic() || ft.NumIn() != 2 {
		fail("handler must accept exactly 2 arguments (context.Context, payload)")
	}
	if ft.In(0) != reflect.TypeFor[context.Context]() {
		fail("handler first argument must be context.Context")
	}
	if ft.NumOut() < 1 || ft.NumOut() > 2 || ft.Out(ft.NumOut()-1) != reflect.TypeFor[error]() {
		fail("handler must return error or (result, error)")
	}
	if ft.NumOut() == 2 && ft.Out(0).Implements(reflect.TypeFor[error]()) {
		fail("handler result must not be another error")
	}
	pt := ft.In(1)
	if pt.Implements(reflect.TypeFor[context.Context]()) || pt.Implements(reflect.TypeFor[http.ResponseWriter]()) {
		fail("payload must be data, not a context or HTTP response writer")
	}
	if err := bindingtype.ValidatePayload(pt); err != nil {
		fail(err.Error())
	}
	if ft.NumOut() == 2 {
		if err := bindingtype.ValidateResult(ft.Out(0)); err != nil {
			fail(err.Error())
		}
	}
	pt, _ = bindingtype.Base(pt) // ValidatePayload already checked all pointer chains.
	if cardinality == "many" && (pt.Kind() != reflect.Slice || bindingtype.IsBytes(pt)) {
		fail("cardinality many requires a slice payload such as []Order or [][]byte, not a single []byte")
	}
}

// The low-level RegisterFunction path shares validation with GenericTrigger so
// a library can implement Bind without bypassing the generic payload contract.
func validateGenericRegistration(rf *RegisteredFunction) {
	fail := func(message string) { panic(fmt.Sprintf("GenericTrigger %q: %s", rf.FuncName, message)) }
	trigger := rf.RawBindings[0]
	if strings.TrimSpace(trigger.Type) == "" {
		fail("type must not be empty")
	}
	if strings.TrimSpace(trigger.Type) != trigger.Type || !strings.HasSuffix(strings.ToLower(trigger.Type), "trigger") {
		fail("type must end in Trigger and match an installed host extension")
	}
	if strings.EqualFold(trigger.Type, "httpTrigger") {
		fail("use App.HTTP for HTTP triggers")
	}
	if trigger.Type != rf.TriggerType || trigger.Direction != "in" {
		fail("trigger type and direction must match registration")
	}
	if rf.ClientFactory != nil {
		fail("SDK client injection requires a typed trigger adapter")
	}
	validateGenericHandler(rf.Func, trigger.GenericBinding.Cardinality)
	names := make(map[string]bool, len(rf.RawBindings))
	for i := range rf.RawBindings {
		b := &rf.RawBindings[i]
		key := strings.ToLower(b.Name)
		isReturn := b.Name == "$return" && b.Direction == "out"
		if (!isReturn && !genericBindingName.MatchString(b.Name)) || names[key] {
			fail("binding name must be valid and unique: " + b.Name)
		}
		names[key] = true
		if strings.TrimSpace(b.Type) == "" {
			fail("binding type must not be empty")
		}
		if i > 0 && strings.HasSuffix(strings.ToLower(b.Type), "trigger") {
			fail("only one trigger is allowed")
		}
		if b.Direction != "in" && !isReturn {
			fail("only input and $return output bindings are supported")
		}
		snapshot, err := snapshotBinding(*b)
		if err != nil {
			fail(err.Error())
		}
		*b = snapshot
	}
}

// snapshotBinding validates serialization once and reuses those exact bytes to
// snapshot generic properties. Custom marshalers run once, and large numbers
// and nested values survive without retaining caller-owned maps or slices.
func snapshotBinding(b bindings.Binding) (bindings.Binding, error) {
	data, err := json.Marshal(b)
	if err != nil {
		return bindings.Binding{}, err
	}
	if b.GenericBinding == nil {
		return b, nil
	}
	var properties map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&properties); err != nil {
		return bindings.Binding{}, err
	}
	for _, key := range []string{"name", "type", "direction", "dataType", "cardinality"} {
		delete(properties, key)
	}
	g := *b.GenericBinding
	g.Properties = properties
	b.GenericBinding = &g
	return b, nil
}
