package bindings

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCosmosDBTrigger_ToBinding(t *testing.T) {
	trigger := &CosmosDBTrigger{
		Name:                            "docs",
		DatabaseName:                    "mydb",
		ContainerName:                   "mycontainer",
		Connection:                      "CosmosDBConnection",
		CreateLeaseContainerIfNotExists: true,
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
	if !binding.CosmosDBBinding.CreateLeaseContainerIfNotExists {
		t.Error("expected CreateLeaseContainerIfNotExists to be propagated as true")
	}
}

func TestCosmosDBTrigger_ToBinding_DefaultsLeaseContainerFalse(t *testing.T) {
	trigger := &CosmosDBTrigger{Name: "docs"}
	binding := trigger.ToBinding()
	if binding.CosmosDBBinding == nil {
		t.Fatal("expected CosmosDBBinding")
	}
	if binding.CosmosDBBinding.CreateLeaseContainerIfNotExists {
		t.Error("expected CreateLeaseContainerIfNotExists default to be false")
	}
}

func TestCosmosDBBinding_JSON_OmitsCreateLeaseContainerWhenFalse(t *testing.T) {
	trigger := &CosmosDBTrigger{
		Name:          "docs",
		DatabaseName:  "mydb",
		ContainerName: "mycontainer",
		Connection:    "CosmosDBConnection",
	}
	data, err := json.Marshal(trigger.ToBinding())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if contains := string(data); strings.Contains(contains, "createLeaseContainerIfNotExists") {
		t.Errorf("expected createLeaseContainerIfNotExists to be omitted when false; got %s", contains)
	}
}

func TestCosmosDBBinding_JSON_EmitsCreateLeaseContainerWhenTrue(t *testing.T) {
	trigger := &CosmosDBTrigger{
		Name:                            "docs",
		DatabaseName:                    "mydb",
		ContainerName:                   "mycontainer",
		Connection:                      "CosmosDBConnection",
		CreateLeaseContainerIfNotExists: true,
	}
	data, err := json.Marshal(trigger.ToBinding())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	v, ok := decoded["createLeaseContainerIfNotExists"]
	if !ok {
		t.Fatalf("expected createLeaseContainerIfNotExists key in %s", string(data))
	}
	if b, _ := v.(bool); !b {
		t.Errorf("expected createLeaseContainerIfNotExists=true, got %v", v)
	}
}

func TestCosmosDocument_JSON(t *testing.T) {
	doc := CosmosDocument{
		ID:        "doc-123",
		Data:      json.RawMessage(`{"key":"value"}`),
		Etag:      "etag-1",
		Timestamp: "1234567890",
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

func TestCosmosDBTrigger(t *testing.T) {
	trigger := &CosmosDBTrigger{}
	if trigger.GetBindingType() != CosmosDBTriggerType {
		t.Errorf("expected %q, got %q", CosmosDBTriggerType, trigger.GetBindingType())
	}
}

func TestCosmosDBTrigger_ToBinding_PropagatesAllFields(t *testing.T) {
	trigger := &CosmosDBTrigger{
		Name:                            "docs",
		DatabaseName:                    "mydb",
		ContainerName:                   "mycontainer",
		Connection:                      "CosmosDBConnection",
		LeaseContainerName:              "myleases",
		LeaseDatabaseName:               "leasesdb",
		LeaseConnection:                 "LeaseConnection",
		CreateLeaseContainerIfNotExists: true,
		LeasesContainerThroughput:       1000,
		LeaseContainerPrefix:            "triggerA_",
		FeedPollDelay:                   2000,
		LeaseRenewInterval:              20000,
		LeaseAcquireInterval:            15000,
		LeaseExpirationInterval:         90000,
		MaxItemsPerInvocation:           50,
		StartFromBeginning:              true,
		StartFromTime:                   "2021-02-16T14:19:29Z",
		PreferredLocations:              "East US,North Europe",
		ChangeFeedMode:                  CosmosChangeFeedModeAllVersionsAndDeletes,
	}
	b := trigger.ToBinding().CosmosDBBinding
	if b == nil {
		t.Fatal("expected CosmosDBBinding")
	}
	checks := []struct {
		name string
		got  any
		want any
	}{
		{"LeaseContainerName", b.LeaseContainerName, "myleases"},
		{"LeaseDatabaseName", b.LeaseDatabaseName, "leasesdb"},
		{"LeaseConnection", b.LeaseConnection, "LeaseConnection"},
		{"LeasesContainerThroughput", b.LeasesContainerThroughput, 1000},
		{"LeaseContainerPrefix", b.LeaseContainerPrefix, "triggerA_"},
		{"FeedPollDelay", b.FeedPollDelay, 2000},
		{"LeaseRenewInterval", b.LeaseRenewInterval, 20000},
		{"LeaseAcquireInterval", b.LeaseAcquireInterval, 15000},
		{"LeaseExpirationInterval", b.LeaseExpirationInterval, 90000},
		{"MaxItemsPerInvocation", b.MaxItemsPerInvocation, 50},
		{"StartFromBeginning", b.StartFromBeginning, true},
		{"StartFromTime", b.StartFromTime, "2021-02-16T14:19:29Z"},
		{"PreferredLocations", b.PreferredLocations, "East US,North Europe"},
		{"ChangeFeedMode", b.ChangeFeedMode, CosmosChangeFeedModeAllVersionsAndDeletes},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, c.got, c.want)
		}
	}
}

// TestCosmosDBBinding_JSON_OmitsOptionalFieldsWhenZero verifies that every
// optional lease/feed knob is dropped from the serialized binding when left
// at its zero value, keeping bindings written by callers that don't opt in
// byte-identical to pre-existing output.
func TestCosmosDBBinding_JSON_OmitsOptionalFieldsWhenZero(t *testing.T) {
	trigger := &CosmosDBTrigger{
		Name:          "docs",
		DatabaseName:  "mydb",
		ContainerName: "mycontainer",
		Connection:    "CosmosDBConnection",
	}
	data, err := json.Marshal(trigger.ToBinding())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	omitted := []string{
		"leaseContainerName",
		"leaseDatabaseName",
		"leaseConnection",
		"createLeaseContainerIfNotExists",
		"leasesContainerThroughput",
		"leaseContainerPrefix",
		"feedPollDelay",
		"leaseRenewInterval",
		"leaseAcquireInterval",
		"leaseExpirationInterval",
		"maxItemsPerInvocation",
		"startFromBeginning",
		"startFromTime",
		"preferredLocations",
		"changeFeedMode",
	}
	for _, key := range omitted {
		if strings.Contains(string(data), key) {
			t.Errorf("expected %q to be omitted when zero-valued; got %s", key, string(data))
		}
	}
}

// TestCosmosDBBinding_JSON_EmitsOptionalFieldsWhenSet locks in the exact JSON
// property names the Functions host expects when each optional field is set.
func TestCosmosDBBinding_JSON_EmitsOptionalFieldsWhenSet(t *testing.T) {
	trigger := &CosmosDBTrigger{
		Name:                            "docs",
		DatabaseName:                    "mydb",
		ContainerName:                   "mycontainer",
		Connection:                      "CosmosDBConnection",
		LeaseContainerName:              "myleases",
		LeaseDatabaseName:               "leasesdb",
		LeaseConnection:                 "LeaseConnection",
		CreateLeaseContainerIfNotExists: true,
		LeasesContainerThroughput:       1000,
		LeaseContainerPrefix:            "triggerA_",
		FeedPollDelay:                   2000,
		LeaseRenewInterval:              20000,
		LeaseAcquireInterval:            15000,
		LeaseExpirationInterval:         90000,
		MaxItemsPerInvocation:           50,
		StartFromBeginning:              true,
		StartFromTime:                   "2021-02-16T14:19:29Z",
		PreferredLocations:              "East US,North Europe",
		ChangeFeedMode:                  CosmosChangeFeedModeAllVersionsAndDeletes,
	}
	data, err := json.Marshal(trigger.ToBinding())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := map[string]any{
		"leaseContainerName":              "myleases",
		"leaseDatabaseName":               "leasesdb",
		"leaseConnection":                 "LeaseConnection",
		"createLeaseContainerIfNotExists": true,
		"leasesContainerThroughput":       float64(1000),
		"leaseContainerPrefix":            "triggerA_",
		"feedPollDelay":                   float64(2000),
		"leaseRenewInterval":              float64(20000),
		"leaseAcquireInterval":            float64(15000),
		"leaseExpirationInterval":         float64(90000),
		"maxItemsPerInvocation":           float64(50),
		"startFromBeginning":              true,
		"startFromTime":                   "2021-02-16T14:19:29Z",
		"preferredLocations":              "East US,North Europe",
		"changeFeedMode":                  "AllVersionsAndDeletes",
	}
	for key, w := range want {
		got, ok := decoded[key]
		if !ok {
			t.Errorf("expected key %q in %s", key, string(data))
			continue
		}
		if got != w {
			t.Errorf("%s: got %v (%T), want %v (%T)", key, got, got, w, w)
		}
	}
}
