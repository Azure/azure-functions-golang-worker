package worker

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/azure/azure-functions-golang-worker/sdk/bindings"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

func TestConvertToTypeValue_CosmosBatch(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_String_{String_: `[
		{"id":"doc-123","data":"hello","_ts":1785870475,"_lsn":5}
	]`}}

	value, err := convertToTypeValue(reflect.TypeOf([]bindings.CosmosDocument{}), data, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	documents := value.Interface().([]bindings.CosmosDocument)
	if len(documents) != 1 {
		t.Fatalf("expected 1 document, got %d", len(documents))
	}
	if documents[0].Timestamp != 1785870475 {
		t.Errorf("expected timestamp %d, got %d", int64(1785870475), documents[0].Timestamp)
	}
}

func TestConvertToTypeValue_EventHubBatch(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_CollectionBytes{
		CollectionBytes: &pb.CollectionBytes{Bytes: [][]byte{[]byte(`{"id":1}`), []byte(`{"id":2}`)}},
	}}
	metadata := map[string]*pb.TypedData{
		"SequenceNumberArray": {
			Data: &pb.TypedData_CollectionSint64{
				CollectionSint64: &pb.CollectionSInt64{Sint64: []int64{10, 11}},
			},
		},
		"OffsetArray": {
			Data: &pb.TypedData_CollectionString{
				CollectionString: &pb.CollectionString{String_: []string{"100", "200"}},
			},
		},
		"PropertiesArray": {
			Data: &pb.TypedData_Json{Json: `[{"source":"first"},{"source":"second"}]`},
		},
	}

	value, err := convertToTypeValue(reflect.TypeOf([]bindings.EventHubMessage{}), data, metadata)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	events := value.Interface().([]bindings.EventHubMessage)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if string(events[0].Body) != `{"id":1}` || events[1].SequenceNumber != 11 || events[1].Offset != "200" {
		t.Fatalf("unexpected events: %+v", events)
	}
	if events[0].Properties["source"] != "first" {
		t.Fatalf("unexpected properties: %+v", events[0].Properties)
	}
}

func TestConvertToTypeValue_EventHubBatchPreservesEmptyEntryAlignment(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_CollectionBytes{
		CollectionBytes: &pb.CollectionBytes{Bytes: [][]byte{[]byte("first"), {}, []byte("third")}},
	}}
	metadata := map[string]*pb.TypedData{
		"SequenceNumberArray": {
			Data: &pb.TypedData_CollectionSint64{
				CollectionSint64: &pb.CollectionSInt64{Sint64: []int64{10, 11, 12}},
			},
		},
		"OffsetArray": {
			Data: &pb.TypedData_CollectionString{
				CollectionString: &pb.CollectionString{String_: []string{"100", "", "300"}},
			},
		},
	}

	value, err := convertToTypeValue(reflect.TypeOf([]bindings.EventHubMessage{}), data, metadata)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	events := value.Interface().([]bindings.EventHubMessage)
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}

	expectedBodies := []string{"first", "", "third"}
	expectedSequenceNumbers := []int64{10, 11, 12}
	expectedOffsets := []string{"100", "", "300"}
	for i, event := range events {
		if string(event.Body) != expectedBodies[i] {
			t.Errorf("event %d: expected body %q, got %q", i, expectedBodies[i], event.Body)
		}
		if event.SequenceNumber != expectedSequenceNumbers[i] {
			t.Errorf("event %d: expected sequence number %d, got %d", i, expectedSequenceNumbers[i], event.SequenceNumber)
		}
		if event.Offset != expectedOffsets[i] {
			t.Errorf("event %d: expected offset %q, got %q", i, expectedOffsets[i], event.Offset)
		}
	}
}

func TestConvertToTypeValue_ServiceBusBatch(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_CollectionString{
		CollectionString: &pb.CollectionString{String_: []string{`"first"`, `"second"`}},
	}}
	metadata := map[string]*pb.TypedData{
		"MessageIdArray": {
			Data: &pb.TypedData_CollectionString{
				CollectionString: &pb.CollectionString{String_: []string{"id-1", "id-2"}},
			},
		},
		"DeliveryCountArray": {
			Data: &pb.TypedData_Json{Json: `[1,2]`},
		},
		"ApplicationPropertiesArray": {
			Data: &pb.TypedData_Json{Json: `[{"priority":"low"},{"priority":"high"}]`},
		},
	}

	value, err := convertToTypeValue(reflect.TypeOf([]bindings.ServiceBusMessage{}), data, metadata)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	messages := value.Interface().([]bindings.ServiceBusMessage)
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}
	if string(messages[0].Body) != `"first"` || messages[1].MessageId != "id-2" || messages[1].DeliveryCount != 2 {
		t.Fatalf("unexpected messages: %+v", messages)
	}
	if messages[1].ApplicationProperties["priority"] != "high" {
		t.Fatalf("unexpected application properties: %+v", messages[1].ApplicationProperties)
	}

	var body string
	if err := json.Unmarshal(messages[1].Body, &body); err != nil || body != "second" {
		t.Fatalf("unexpected second body %q: %v", messages[1].Body, err)
	}
}

func TestConvertToTypeValue_BatchMetadataArrayOverridesScalar(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_CollectionBytes{
		CollectionBytes: &pb.CollectionBytes{Bytes: [][]byte{[]byte("first"), []byte("second")}},
	}}
	metadata := map[string]*pb.TypedData{
		"MessageId": {
			Data: &pb.TypedData_String_{String_: "scalar-id"},
		},
		"MessageIdArray": {
			Data: &pb.TypedData_CollectionString{
				CollectionString: &pb.CollectionString{String_: []string{"id-1", "id-2"}},
			},
		},
	}

	value, err := convertToTypeValue(reflect.TypeOf([]bindings.ServiceBusMessage{}), data, metadata)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	messages := value.Interface().([]bindings.ServiceBusMessage)
	if messages[0].MessageId != "id-1" || messages[1].MessageId != "id-2" {
		t.Fatalf("expected per-message IDs to override scalar metadata, got %+v", messages)
	}
}

func TestConvertToTypeValue_BatchSkipsMalformedArrayMetadata(t *testing.T) {
	data := &pb.TypedData{Data: &pb.TypedData_CollectionBytes{
		CollectionBytes: &pb.CollectionBytes{Bytes: [][]byte{[]byte("first"), []byte("second")}},
	}}
	metadata := map[string]*pb.TypedData{
		"MessageIdArray": {
			Data: &pb.TypedData_CollectionString{
				CollectionString: &pb.CollectionString{String_: []string{"id-1", "id-2"}},
			},
		},
		"SomeWeirdArray": {
			Data: &pb.TypedData_Json{Json: `{"not":"an array"}`},
		},
	}

	value, err := convertToTypeValue(reflect.TypeOf([]bindings.ServiceBusMessage{}), data, metadata)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	messages := value.Interface().([]bindings.ServiceBusMessage)
	if len(messages) != 2 || messages[0].MessageId != "id-1" || messages[1].MessageId != "id-2" {
		t.Fatalf("unexpected messages: %+v", messages)
	}
}
