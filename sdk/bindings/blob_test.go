package bindings

import (
	"encoding/json"
	"testing"
)

func TestBlobTrigger_ToBinding(t *testing.T) {
	trigger := &BlobTrigger{
		Name:       "blob",
		Path:       "container/{name}",
		Connection: "AzureWebJobsStorage",
	}

	binding := trigger.ToBinding()

	if binding.Type != "blobTrigger" {
		t.Errorf("expected type %q, got %q", "blobTrigger", binding.Type)
	}
	if binding.Direction != "in" {
		t.Errorf("expected direction %q, got %q", "in", binding.Direction)
	}
	if binding.BlobBinding == nil {
		t.Fatal("expected BlobBinding")
	}
	if binding.BlobBinding.Path != "container/{name}" {
		t.Errorf("expected path %q, got %q", "container/{name}", binding.BlobBinding.Path)
	}
	if binding.BlobBinding.Connection != "AzureWebJobsStorage" {
		t.Errorf("expected connection %q, got %q", "AzureWebJobsStorage", binding.BlobBinding.Connection)
	}
}

func TestBlobTrigger(t *testing.T) {
	trigger := &BlobTrigger{}
	if trigger.GetBindingType() != BlobTriggerType {
		t.Errorf("expected %q, got %q", BlobTriggerType, trigger.GetBindingType())
	}
}

func TestBlobBinding_MarshalJSON(t *testing.T) {
	binding := Binding{
		Name:      "blob",
		Type:      "blobTrigger",
		Direction: "in",
		BlobBinding: &BlobBinding{
			Path:       "test-container/{name}",
			Connection: "StorageConn",
		},
	}

	data, err := json.Marshal(binding)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result["path"] != "test-container/{name}" {
		t.Errorf("expected path in JSON, got %v", result)
	}
	if result["connection"] != "StorageConn" {
		t.Errorf("expected connection in JSON, got %v", result)
	}
}
