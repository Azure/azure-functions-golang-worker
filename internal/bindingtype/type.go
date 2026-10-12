// Package bindingtype holds the type rules shared by generic registration,
// input decoding, and return encoding.
package bindingtype

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
)

var (
	numberType          = reflect.TypeFor[json.Number]()
	jsonMarshalerType   = reflect.TypeFor[json.Marshaler]()
	textMarshalerType   = reflect.TypeFor[encoding.TextMarshaler]()
	jsonUnmarshalerType = reflect.TypeFor[json.Unmarshaler]()
	textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()
)

// IsText reports whether t is exchanged as raw text. json.Number is numeric JSON.
func IsText(t reflect.Type) bool { return t.Kind() == reflect.String && t != numberType }

// IsBytes reports whether t is exchanged as raw bytes.
func IsBytes(t reflect.Type) bool {
	return t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8
}

// IsRaw reports whether t is exchanged without JSON conversion.
func IsRaw(t reflect.Type) bool { return IsText(t) || IsBytes(t) }

// Marshals reports whether values of t provide their own JSON or text encoding.
func Marshals(t reflect.Type) bool {
	return t.Implements(jsonMarshalerType) || t.Implements(textMarshalerType)
}

func unmarshals(t reflect.Type) bool {
	p := reflect.PointerTo(t)
	return p.Implements(jsonUnmarshalerType) || p.Implements(textUnmarshalerType)
}

// Base unwraps pointers, rejecting pointer-only cycles which have no data type.
func Base(t reflect.Type) (reflect.Type, error) {
	seen := make(map[reflect.Type]bool)
	for t.Kind() == reflect.Ptr {
		if seen[t] {
			return nil, fmt.Errorf("recursive pointer type %s holds no data", t)
		}
		seen[t] = true
		t = t.Elem()
	}
	return t, nil
}

// ValidatePayload rejects a payload type that neither raw delivery nor
// encoding/json can populate. See validateShape for the rules. Pointer-only
// cycles are rejected in every field because decoding them never terminates.
func ValidatePayload(t reflect.Type) error {
	if err := rejectPointerCycles(t); err != nil {
		return fmt.Errorf("unsupported payload type %s: %w", t, err)
	}
	return validateShape(t, "payload", unmarshals, func(key reflect.Type) bool {
		return reflect.PointerTo(key).Implements(textUnmarshalerType)
	})
}

// ValidateResult rejects a result type that neither raw delivery nor
// encoding/json can encode. See validateShape for the rules. A returned value
// is not addressable, so only a pointer result can use pointer methods. Nested
// values may be addressable, so a marshaler on T or *T counts there.
func ValidateResult(t reflect.Type) error {
	root := t
	err := validateShape(t, "result", func(t reflect.Type) bool {
		return Marshals(t) || (t != root && Marshals(reflect.PointerTo(t)))
	}, func(key reflect.Type) bool {
		return key.Implements(textMarshalerType)
	})
	if err != nil && t.Kind() != reflect.Pointer && Marshals(reflect.PointerTo(t)) {
		return fmt.Errorf("%w; return %s so its pointer-receiver marshaler applies", err, reflect.PointerTo(t))
	}
	return err
}

// validateShape applies the documented encoding/json limits to pointers,
// slices, arrays, and maps. A type with a custom codec owns its conversion, and
// struct fields are left to encoding/json instead of copying its field rules.
// Only payloads reject non-empty interfaces, which JSON cannot populate.
func validateShape(root reflect.Type, role string, custom, textKey func(reflect.Type) bool) error {
	seen := make(map[reflect.Type]bool)
	var visit func(reflect.Type) error
	visit = func(t reflect.Type) error {
		base, err := Base(t)
		if err != nil {
			return err
		}
		if seen[base] || custom(base) {
			return nil
		}
		seen[base] = true
		switch base.Kind() {
		case reflect.Chan, reflect.Func, reflect.Complex64, reflect.Complex128, reflect.UnsafePointer:
			return fmt.Errorf("%s cannot be represented in JSON", base)
		case reflect.Interface:
			if role == "payload" && base.NumMethod() > 0 {
				return fmt.Errorf("JSON cannot populate non-empty interface %s", base)
			}
		case reflect.Map:
			switch key := base.Key(); key.Kind() {
			case reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			default:
				if !textKey(key) {
					return fmt.Errorf("map key %s must be a string, an integer, or have a text encoding", key)
				}
			}
			return visit(base.Elem())
		case reflect.Slice, reflect.Array:
			return visit(base.Elem())
		}
		return nil
	}
	if err := visit(root); err != nil {
		return fmt.Errorf("unsupported %s type %s: %w", role, root, err)
	}
	return nil
}

// rejectPointerCycles visits every struct field, including ones encoding/json
// ignores, so the check never depends on JSON field visibility rules.
func rejectPointerCycles(root reflect.Type) error {
	seen := make(map[reflect.Type]bool)
	var visit func(reflect.Type) error
	visit = func(t reflect.Type) error {
		base, err := Base(t)
		if err != nil || seen[base] {
			return err
		}
		seen[base] = true
		switch base.Kind() {
		case reflect.Slice, reflect.Array:
			return visit(base.Elem())
		case reflect.Map:
			if err := visit(base.Key()); err != nil {
				return err
			}
			return visit(base.Elem())
		case reflect.Struct:
			for i := range base.NumField() {
				if err := visit(base.Field(i).Type); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return visit(root)
}
