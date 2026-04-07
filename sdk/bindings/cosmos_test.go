package bindings

import (
	"encoding/json"
	"testing"
)

func TestCosmosDBTrigger_ToBinding(t *testing.T) {
	trigger := &CosmosDBTrigger{
		Name:          "docs",
		DatabaseName:  "mydb",
		ContainerName: "mycontainer",
		Connection:    "CosmosDBConnection",
	}

	binding := trigger.ToBinding()

	if binding.Type != "cosmosDBTrigger" {
		t.Errorf("expected type %q, got %q", "cosmosDBTrigger", binding.Type)
	}
	if binding.Direction != "in" {
		t.Errorf("expected direction %q, got %q", "in", binding.Direction)
	}
	if binding.CosmosDBBinding == nil {
		t.Fatal("expected CosmosDBBinding")
	}
	if binding.CosmosDBBinding.DatabaseName != "mydb" {
		t.Errorf("expected database %q, got %q", "mydb", binding.CosmosDBBinding.DatabaseName)
	}
	if binding.CosmosDBBinding.ContainerName != "mycontainer" {
		t.Errorf("expected container %q, got %q", "mycontainer", binding.CosmosDBBinding.ContainerName)
	}
	if binding.CosmosDBBinding.Connection != "CosmosDBConnection" {
		t.Errorf("expected connection %q, got %q", "CosmosDBConnection", binding.CosmosDBBinding.Connection)
	}
}

func TestCosmosDocument_JSON(t *testing.T) {
	doc := CosmosDocument{
		ID:        "doc-123",
		Data:      json.RawMessage(`{"key":"value"}`),
		Etag:      "etag-1",
		Timestamp: 1234567890,
	}

	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded CosmosDocument
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if decoded.ID != "doc-123" {
		t.Errorf("expected id %q, got %q", "doc-123", decoded.ID)
	}
	if string(decoded.Data) != `{"key":"value"}` {
		t.Errorf("expected data %q, got %q", `{"key":"value"}`, string(decoded.Data))
	}
}

func TestCosmosDBTriggerBindingType(t *testing.T) {
	trigger := &CosmosDBTrigger{}
	if trigger.GetBindingType() != CosmosDBTriggerBindingType {
		t.Errorf("expected %q, got %q", CosmosDBTriggerBindingType, trigger.GetBindingType())
	}
}
