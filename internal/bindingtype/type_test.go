package bindingtype

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"
)

type loop *loop
type node struct {
	Next     *node
	Children []*node
}
type hiddenLoop struct{ Next loop }
type promotedLoop struct{ hiddenLoop }
type dashNamedLoop struct {
	Value loop `json:"-,"`
}
type namedText string
type namedByte uint8
type textKey struct{ Value string }

func (k *textKey) UnmarshalText(b []byte) error { k.Value = string(b); return nil }
func (k textKey) MarshalText() ([]byte, error)  { return []byte(k.Value), nil }

type decodedEvents chan int

func (*decodedEvents) UnmarshalJSON([]byte) error { return nil }

type encodedEvents chan int

func (encodedEvents) MarshalJSON() ([]byte, error) { return []byte(`"events"`), nil }

// encoding/json uses this pointer-receiver method for addressable slice elements.
type pointerScores map[float64]string

func (*pointerScores) MarshalJSON() ([]byte, error) { return []byte(`{}`), nil }

type tree map[string]tree

func TestValidatePayloadRejectsPointerCycles(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeFor[loop](), reflect.TypeFor[[]loop](), reflect.TypeFor[map[string]loop](), reflect.TypeFor[struct{ Value loop }](),
		// encoding/json decodes both fields, and a pointer-only cycle never terminates.
		reflect.TypeFor[promotedLoop](), reflect.TypeFor[dashNamedLoop](),
	} {
		if err := ValidatePayload(typ); err == nil || !strings.Contains(err.Error(), "recursive") {
			t.Errorf("%s: error = %v, want recursive pointer rejection", typ, err)
		}
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[node](), reflect.TypeFor[[]*node](), reflect.TypeFor[**string](), reflect.TypeFor[map[string]any]()} {
		if err := ValidatePayload(typ); err != nil {
			t.Errorf("rejected valid type %s: %v", typ, err)
		}
	}
}

func TestRawClassification(t *testing.T) {
	for _, tt := range []struct {
		typ         reflect.Type
		text, bytes bool
	}{
		{reflect.TypeFor[string](), true, false},
		{reflect.TypeFor[namedText](), true, false},
		{reflect.TypeFor[json.Number](), false, false},
		{reflect.TypeFor[[]byte](), false, true},
		{reflect.TypeFor[json.RawMessage](), false, true},
		{reflect.TypeFor[[]namedByte](), false, true},
		{reflect.TypeFor[[4]byte](), false, false},
		{reflect.TypeFor[[]string](), false, false},
		{reflect.TypeFor[*string](), false, false},
	} {
		if IsText(tt.typ) != tt.text || IsBytes(tt.typ) != tt.bytes || IsRaw(tt.typ) != (tt.text || tt.bytes) {
			t.Errorf("%s: text=%v bytes=%v raw=%v, want text=%v bytes=%v", tt.typ, IsText(tt.typ), IsBytes(tt.typ), IsRaw(tt.typ), tt.text, tt.bytes)
		}
	}
}

func TestValidatePayloadShapes(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeFor[string](), reflect.TypeFor[[]byte](), reflect.TypeFor[*string](), reflect.TypeFor[json.Number](),
		reflect.TypeFor[json.RawMessage](), reflect.TypeFor[time.Time](), reflect.TypeFor[any](), reflect.TypeFor[[]any](),
		reflect.TypeFor[map[string]any](), reflect.TypeFor[map[int64]string](), reflect.TypeFor[map[textKey]string](),
		reflect.TypeFor[[2]int](), reflect.TypeFor[decodedEvents](), reflect.TypeFor[tree](),
		// Struct fields are left to encoding/json rather than copying its field rules.
		reflect.TypeFor[struct{ Body io.Reader }](),
	} {
		if err := ValidatePayload(typ); err != nil {
			t.Errorf("rejected decodable type %s: %v", typ, err)
		}
	}
	for typ, want := range map[reflect.Type]string{
		reflect.TypeFor[io.Reader]():           "io.Reader",
		reflect.TypeFor[[]io.Reader]():         "io.Reader",
		reflect.TypeFor[map[float64]string]():  "map key float64",
		reflect.TypeFor[chan int]():            "chan int",
		reflect.TypeFor[*chan int]():           "chan int",
		reflect.TypeFor[func()]():              "func()",
		reflect.TypeFor[complex128]():          "complex128",
		reflect.TypeFor[unsafe.Pointer]():      "unsafe.Pointer",
		reflect.TypeFor[map[string][]func()](): "func()",
		reflect.TypeFor[encodedEvents]():       "encodedEvents",
	} {
		err := ValidatePayload(typ)
		if err == nil || !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "payload") {
			t.Errorf("%s: error = %v, want payload error containing %q", typ, err, want)
		}
	}
}

func TestValidateResultShapes(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeFor[string](), reflect.TypeFor[*string](), reflect.TypeFor[[]byte](), reflect.TypeFor[*[]byte](),
		reflect.TypeFor[json.Number](), reflect.TypeFor[any](), reflect.TypeFor[fmt.Stringer](), reflect.TypeFor[map[textKey]string](),
		reflect.TypeFor[encodedEvents](), reflect.TypeFor[tree](), reflect.TypeFor[[2]string](),
		reflect.TypeFor[[]pointerScores](), reflect.TypeFor[*pointerScores](),
		reflect.TypeFor[struct{ Done chan int }](),
	} {
		if err := ValidateResult(typ); err != nil {
			t.Errorf("rejected encodable type %s: %v", typ, err)
		}
	}
	for typ, want := range map[reflect.Type]string{
		reflect.TypeFor[chan int]():           "chan int",
		reflect.TypeFor[*func()]():            "func()",
		reflect.TypeFor[complex64]():          "complex64",
		reflect.TypeFor[map[float64]string](): "map key float64",
		reflect.TypeFor[map[any]string]():     "map key interface {}",
		reflect.TypeFor[[]chan int]():         "chan int",
		reflect.TypeFor[decodedEvents]():      "decodedEvents",
		reflect.TypeFor[loop]():               "recursive",
		// A returned value is never addressable, so its pointer method never runs.
		reflect.TypeFor[pointerScores](): "return *bindingtype.pointerScores",
	} {
		err := ValidateResult(typ)
		if err == nil || !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "result") {
			t.Errorf("%s: error = %v, want result error containing %q", typ, err, want)
		}
	}
}

func TestBase(t *testing.T) {
	if _, err := Base(reflect.TypeFor[loop]()); err == nil {
		t.Fatal("accepted recursive pointer")
	}
	if got, err := Base(reflect.TypeFor[**node]()); err != nil || got != reflect.TypeFor[node]() {
		t.Fatalf("base=%v error=%v", got, err)
	}
}
