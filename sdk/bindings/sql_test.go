package bindings

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSQLTrigger(t *testing.T) {
	trigger := &SQLTrigger{}
	if trigger.GetBindingType() != SQLTriggerType {
		t.Errorf("expected %q, got %q", SQLTriggerType, trigger.GetBindingType())
	}
	if string(SQLTriggerType) != "sqlTrigger" {
		t.Errorf("SQLTriggerType must be the literal %q, got %q",
			"sqlTrigger", SQLTriggerType)
	}
}

func TestSQLTrigger_ToBinding(t *testing.T) {
	trigger := &SQLTrigger{
		Name:                    "changes",
		TableName:               "dbo.Products",
		ConnectionStringSetting: "AzureWebJobsSqlConnectionString",
	}

	binding := trigger.ToBinding()

	if binding.Name != "changes" {
		t.Errorf("expected name %q, got %q", "changes", binding.Name)
	}
	if binding.Type != "sqlTrigger" {
		t.Errorf("expected type %q, got %q", "sqlTrigger", binding.Type)
	}
	if binding.Direction != "in" {
		t.Errorf("expected direction %q, got %q", "in", binding.Direction)
	}
	if binding.SQLBinding == nil {
		t.Fatal("expected SQLBinding")
	}
	if binding.SQLBinding.TableName != "dbo.Products" {
		t.Errorf("expected tableName %q, got %q", "dbo.Products", binding.SQLBinding.TableName)
	}
	if binding.SQLBinding.ConnectionStringSetting != "AzureWebJobsSqlConnectionString" {
		t.Errorf("expected connectionStringSetting %q, got %q",
			"AzureWebJobsSqlConnectionString", binding.SQLBinding.ConnectionStringSetting)
	}
}

// TestSQLBinding_MarshalJSON_Shape locks down the wire format keys to match
// the SQL binding schema (Microsoft.Azure.WebJobs.Extensions.Sql).
func TestSQLBinding_MarshalJSON_Shape(t *testing.T) {
	trigger := &SQLTrigger{
		Name:                    "changes",
		TableName:               "dbo.Products",
		ConnectionStringSetting: "AzureWebJobsSqlConnectionString",
	}
	binding := trigger.ToBinding()

	data, err := json.Marshal(binding)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Exactly the expected keys, nothing more (no nested object, no extras).
	wantKeys := []string{"name", "type", "direction", "tableName", "connectionStringSetting"}
	if len(got) != len(wantKeys) {
		t.Errorf("expected %d top-level keys, got %d: %v", len(wantKeys), len(got), got)
	}
	for _, k := range wantKeys {
		if _, ok := got[k]; !ok {
			t.Errorf("missing key %q in serialized binding: %s", k, string(data))
		}
	}

	// Values.
	if got["type"] != "sqlTrigger" {
		t.Errorf("expected type %q, got %v", "sqlTrigger", got["type"])
	}
	if got["direction"] != "in" {
		t.Errorf("expected direction %q, got %v", "in", got["direction"])
	}
	if got["tableName"] != "dbo.Products" {
		t.Errorf("expected tableName %q, got %v", "dbo.Products", got["tableName"])
	}
	if got["connectionStringSetting"] != "AzureWebJobsSqlConnectionString" {
		t.Errorf("expected connectionStringSetting %q, got %v",
			"AzureWebJobsSqlConnectionString", got["connectionStringSetting"])
	}
}

// TestSQLChange_JSON_RoundTrip uses the literal wire-format payload shape
// produced by the SQL extension. Field name casing (capital "Operation",
// capital "Item") must match exactly or deserialization will fail.
func TestSQLChange_JSON_RoundTrip(t *testing.T) {
	payload := `[{"Operation":0,"Item":{"ProductId":1,"Name":"Widget","Cost":100}},{"Operation":1,"Item":{"ProductId":2,"Name":"Gadget","Cost":250}},{"Operation":2,"Item":{"ProductId":3,"Name":"Gizmo","Cost":50}}]`

	var changes []SQLChange
	if err := json.Unmarshal([]byte(payload), &changes); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(changes) != 3 {
		t.Fatalf("expected 3 changes, got %d", len(changes))
	}
	if changes[0].Operation != SQLOperationInsert {
		t.Errorf("change[0]: expected Insert, got %v", changes[0].Operation)
	}
	if changes[1].Operation != SQLOperationUpdate {
		t.Errorf("change[1]: expected Update, got %v", changes[1].Operation)
	}
	if changes[2].Operation != SQLOperationDelete {
		t.Errorf("change[2]: expected Delete, got %v", changes[2].Operation)
	}

	type product struct {
		ProductID int    `json:"ProductId"`
		Name      string `json:"Name"`
		Cost      int    `json:"Cost"`
	}
	var p product
	if err := json.Unmarshal(changes[0].Item, &p); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.ProductID != 1 || p.Name != "Widget" || p.Cost != 100 {
		t.Errorf("decoded item mismatch: %+v", p)
	}

	// Re-encode and decode again; the SQLChange shape must survive a round trip.
	roundTrip, err := json.Marshal(changes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var decoded []SQLChange
	if err := json.Unmarshal(roundTrip, &decoded); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(decoded) != 3 || decoded[0].Operation != SQLOperationInsert {
		t.Errorf("round-trip lost data: %s", string(roundTrip))
	}
}

func TestSQLChange_JSON_EmptyBatch(t *testing.T) {
	var changes []SQLChange
	if err := json.Unmarshal([]byte(`[]`), &changes); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(changes) != 0 {
		t.Errorf("expected 0 changes, got %d", len(changes))
	}
}

func TestSQLOperation_String(t *testing.T) {
	cases := []struct {
		op   SQLOperation
		want string
	}{
		{SQLOperationInsert, "Insert"},
		{SQLOperationUpdate, "Update"},
		{SQLOperationDelete, "Delete"},
		{SQLOperation(99), "SQLOperation(99)"},
	}
	for _, c := range cases {
		if got := c.op.String(); got != c.want {
			t.Errorf("SQLOperation(%d).String() = %q, want %q", int(c.op), got, c.want)
		}
	}
}

// TestSQLBinding_MarshalJSON_NoNestedObject verifies that the SQLBinding
// fields get flattened into the top-level JSON object by Binding.MarshalJSON,
// not serialized as a nested "SQLBinding": {...} key.
func TestSQLBinding_MarshalJSON_NoNestedObject(t *testing.T) {
	trigger := &SQLTrigger{
		Name:                    "changes",
		TableName:               "dbo.Products",
		ConnectionStringSetting: "AzureWebJobsSqlConnectionString",
	}
	data, err := json.Marshal(trigger.ToBinding())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(string(data), "SQLBinding") {
		t.Errorf("expected SQLBinding fields to be flattened, but got nested key in: %s",
			string(data))
	}
}
