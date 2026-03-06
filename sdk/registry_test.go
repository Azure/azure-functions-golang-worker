package sdk

import (
	"context"
	"reflect"
	"testing"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

type testCustomType struct {
	Value string
}

func TestRegisterAndGetConverter(t *testing.T) {
	targetType := (*testCustomType)(nil)

	converter := func(ctx context.Context, config map[string]interface{}, data *pb.TypedData, metadata map[string]*pb.TypedData) (reflect.Value, error) {
		return reflect.ValueOf(&testCustomType{Value: "custom"}), nil
	}

	RegisterConverter(targetType, converter)

	fn, found := GetConverter(reflect.TypeOf(targetType))
	if !found {
		t.Fatal("expected converter to be found")
	}

	result, err := fn(context.Background(), nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	obj := result.Interface().(*testCustomType)
	if obj.Value != "custom" {
		t.Errorf("expected value %q, got %q", "custom", obj.Value)
	}
}

func TestGetConverter_NotFound(t *testing.T) {
	type unknownType struct{}

	_, found := GetConverter(reflect.TypeOf(unknownType{}))
	if found {
		t.Error("expected converter NOT to be found for unregistered type")
	}
}
