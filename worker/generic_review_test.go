package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

type reviewPointerLoop *reviewPointerLoop
type validatedOrders []genericOrder

func (v *validatedOrders) UnmarshalJSON(data []byte) error {
	type plainOrders []genericOrder
	var orders plainOrders
	if err := json.Unmarshal(data, &orders); err != nil {
		return err
	}
	for i := range orders {
		if orders[i].ID == "reject" {
			return errors.New("batch rejected")
		}
		orders[i].ID = "validated:" + orders[i].ID
	}
	*v = validatedOrders(orders)
	return nil
}

type validatedTexts []string

func (v *validatedTexts) UnmarshalJSON(data []byte) error {
	type plain []string
	var values plain
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}
	*v = append(validatedTexts{"validated"}, values...)
	return nil
}

type validatedBytes [][]byte

func (v *validatedBytes) UnmarshalJSON(data []byte) error {
	type plain [][]byte
	var values plain
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}
	*v = append(validatedBytes{[]byte("validated")}, values...)
	return nil
}

type failingText string

func (failingText) MarshalText() ([]byte, error) { return nil, errors.New("text rejected") }

type dualMarshaler string

func (dualMarshaler) MarshalJSON() ([]byte, error) { return []byte(`"json wins"`), nil }
func (dualMarshaler) MarshalText() ([]byte, error) { return []byte("text loses"), nil }

type rawCustomText string

func (*rawCustomText) UnmarshalJSON([]byte) error { return errors.New("raw hook must not run") }

type nullAwareBatch []genericOrder

func (b *nullAwareBatch) UnmarshalJSON(data []byte) error {
	if string(data) != "[null]" {
		return errors.New("expected null to reach the batch decoder")
	}
	*b = nullAwareBatch{{ID: "custom-null"}}
	return nil
}

func TestGenericCustomDecoderPrecedence(t *testing.T) {
	for _, data := range []*pb.TypedData{
		{Data: &pb.TypedData_Json{Json: "[null]"}},
		{Data: &pb.TypedData_CollectionString{CollectionString: &pb.CollectionString{String_: []string{"null"}}}},
	} {
		got, err := decodeGenericInput(reflect.TypeFor[nullAwareBatch](), data)
		if err != nil || !reflect.DeepEqual(got.Interface(), nullAwareBatch{{ID: "custom-null"}}) {
			t.Fatalf("custom batch must own null validation: value=%v error=%v", got, err)
		}
	}
	got, err := decodeGenericInput(reflect.TypeFor[rawCustomText](), &pb.TypedData{Data: &pb.TypedData_Json{Json: `"raw"`}})
	if err != nil || got.String() != `"raw"` {
		t.Fatalf("raw scalar contract changed: %v %v", got, err)
	}
}

func TestGenericTypedNumberPrecision(t *testing.T) {
	type model struct {
		ID    int64   `json:"id"`
		Value float64 `json:"value"`
		Extra any     `json:"extra"`
	}
	got, err := decodeGenericInput(reflect.TypeFor[model](), &pb.TypedData{Data: &pb.TypedData_Json{Json: `{"id":9007199254740993,"value":1.5,"extra":9007199254740993}`}})
	want := model{ID: 9007199254740993, Value: 1.5, Extra: json.Number("9007199254740993")}
	if err != nil || !reflect.DeepEqual(got.Interface(), want) {
		t.Fatalf("typed/dynamic fields=%v error=%v", got, err)
	}
	got, err = decodeGenericInput(reflect.TypeFor[json.Number](), &pb.TypedData{Data: &pb.TypedData_Json{Json: `"9007199254740993"`}})
	if err != nil || got.Interface() != json.Number("9007199254740993") {
		t.Fatalf("json.Number follows numeric JSON decoding, not raw-string handling: %v %v", got, err)
	}
}

func TestGenericReturnPreservesTextMarshalers(t *testing.T) {
	for _, value := range []any{net.IPv4(127, 0, 0, 1), net.IP(nil), failingText("x"), dualMarshaler("x"), json.Number("9007199254740993")} {
		t.Run(reflect.TypeOf(value).String(), func(t *testing.T) {
			want, wantErr := json.Marshal(value)
			got, err := encodeReturnValue(value)
			if (err != nil) != (wantErr != nil) {
				t.Fatalf("error=%v, want error=%v", err, wantErr)
			}
			if err == nil && got.GetJson() != string(want) {
				t.Fatalf("wire result=%v, want JSON %s", got, want)
			}
		})
	}
}

func TestGenericCustomBatchDecoder(t *testing.T) {
	for _, id := range []string{"one", "reject"} {
		for _, wire := range []string{"json", "strings", "bytes"} {
			t.Run(id+"/"+wire, func(t *testing.T) {
				payload := `{"id":"` + id + `"}`
				var data *pb.TypedData
				switch wire {
				case "json":
					data = &pb.TypedData{Data: &pb.TypedData_Json{Json: "[" + payload + "]"}}
				case "strings":
					data = &pb.TypedData{Data: &pb.TypedData_CollectionString{CollectionString: &pb.CollectionString{String_: []string{payload}}}}
				case "bytes":
					data = &pb.TypedData{Data: &pb.TypedData_CollectionBytes{CollectionBytes: &pb.CollectionBytes{Bytes: [][]byte{[]byte(payload)}}}}
				}
				var seen validatedOrders
				disp, rf := loadGeneric(t, func(_ context.Context, v validatedOrders) error { seen = v; return nil })
				resp, err := handleInvocationRequest(genericRequest(rf, data), disp, "request")
				if err != nil {
					t.Fatal(err)
				}
				if id == "reject" {
					if resp.GetInvocationResponse().GetResult().GetStatus() != pb.StatusResult_Failure || !strings.Contains(resp.GetInvocationResponse().GetResult().GetException().GetMessage(), "batch rejected") {
						t.Fatal(resp)
					}
				} else if !reflect.DeepEqual(seen, validatedOrders{{ID: "validated:one"}}) {
					t.Fatalf("custom decoder bypassed: %#v", seen)
				}
			})
		}
	}
	for _, tc := range []struct {
		data *pb.TypedData
		want any
	}{
		{&pb.TypedData{Data: &pb.TypedData_CollectionString{CollectionString: &pb.CollectionString{String_: []string{"", `"quoted"`}}}}, validatedTexts{"validated", "", `"quoted"`}},
		{&pb.TypedData{Data: &pb.TypedData_CollectionBytes{CollectionBytes: &pb.CollectionBytes{Bytes: [][]byte{{0, 255}, {}}}}}, validatedBytes{[]byte("validated"), {0, 255}, {}}},
	} {
		got, err := decodeGenericInput(reflect.TypeOf(tc.want), tc.data)
		if err != nil || !reflect.DeepEqual(got.Interface(), tc.want) {
			t.Fatalf("got=%v error=%v want=%v", got, err, tc.want)
		}
	}
}

func TestGenericDynamicNumberPrecision(t *testing.T) {
	const payload = `{"id":9007199254740993,"nested":[1,2.50,1e10]}`
	for _, data := range []*pb.TypedData{
		{Data: &pb.TypedData_Json{Json: payload}}, {Data: &pb.TypedData_String_{String_: payload}}, {Data: &pb.TypedData_Bytes{Bytes: []byte(payload)}},
	} {
		for _, target := range []reflect.Type{reflect.TypeFor[map[string]any](), reflect.TypeFor[any]()} {
			got, err := decodeGenericInput(target, data)
			if err != nil {
				t.Fatal(err)
			}
			values := got.Interface().(map[string]any)
			if values["id"] != json.Number("9007199254740993") || !reflect.DeepEqual(values["nested"], []any{json.Number("1"), json.Number("2.50"), json.Number("1e10")}) {
				t.Fatalf("lossy values: %#v", values)
			}
		}
	}
	got, err := decodeGenericInput(reflect.TypeFor[[]any](), &pb.TypedData{Data: &pb.TypedData_CollectionSint64{CollectionSint64: &pb.CollectionSInt64{Sint64: []int64{9007199254740993}}}})
	if err != nil || !reflect.DeepEqual(got.Interface(), []any{json.Number("9007199254740993")}) {
		t.Fatalf("batch=%v error=%v", got, err)
	}
	for _, payload := range []string{`{} {}`, `{} garbage`, `null 1`} {
		if _, err := decodeGenericInput(reflect.TypeFor[any](), &pb.TypedData{Data: &pb.TypedData_Json{Json: payload}}); err == nil {
			t.Fatalf("accepted trailing content: %s", payload)
		}
	}
}

func TestGenericRawMatchingRepresentationsDoNotCopy(t *testing.T) {
	text := strings.Repeat("x", 1<<20)
	for _, data := range []*pb.TypedData{{Data: &pb.TypedData_String_{String_: text}}, {Data: &pb.TypedData_Json{Json: text}}} {
		value, err := decodeGenericInput(reflect.TypeFor[string](), data)
		if err != nil {
			t.Fatal(err)
		}
		if unsafe.StringData(value.String()) != unsafe.StringData(text) {
			t.Fatal("matching raw string was copied")
		}
	}
	payload := bytes.Repeat([]byte{255}, 1024)
	value, err := decodeGenericInput(reflect.TypeFor[[]byte](), &pb.TypedData{Data: &pb.TypedData_Bytes{Bytes: payload}})
	if err != nil || unsafe.SliceData(value.Interface().([]byte)) != unsafe.SliceData(payload) {
		t.Fatalf("matching bytes copied: %v", err)
	}
}

var genericBenchmarkValue reflect.Value

func BenchmarkGenericRawString(b *testing.B) {
	data := &pb.TypedData{Data: &pb.TypedData_String_{String_: strings.Repeat("x", 1<<20)}}
	for _, size := range []string{"existing", "generic"} {
		b.Run(size, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var err error
				if size == "existing" {
					genericBenchmarkValue, err = convertToTypeValue(reflect.TypeFor[string](), data, nil)
				} else {
					genericBenchmarkValue, err = decodeGenericInput(reflect.TypeFor[string](), data)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestGenericDecoderRejectsNestedPointerCycle(t *testing.T) {
	_, err := decodeGenericInput(reflect.TypeFor[[]reviewPointerLoop](), &pb.TypedData{Data: &pb.TypedData_CollectionString{CollectionString: &pb.CollectionString{String_: []string{"null"}}}})
	if err == nil || !strings.Contains(err.Error(), "recursive") {
		t.Fatalf("expected defensive cycle rejection, got %v", err)
	}
}
