package integration

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
)

const durableBlobConnectionStringEnv = "DURABLE_TEST_BLOB_CONNECTION_STRING"

type blobPayloadValue struct {
	Value string `json:"value"`
}

type blobPayloadRoundTrip struct {
	Input            blobPayloadValue `json:"input"`
	Activity         blobPayloadValue `json:"activity"`
	SubOrchestration blobPayloadValue `json:"subOrchestration"`
	Event            blobPayloadValue `json:"event"`
	Entity           blobPayloadValue `json:"entity"`
	InlineString     string           `json:"inlineString"`
	InlineObject     struct {
		Value string `json:"value"`
	} `json:"inlineObject"`
	Continued bool `json:"continued"`
}

// TestDurableBlobOffloadingRoundTrip opts in to the real AzureBlobStore only
// when DURABLE_TEST_BLOB_CONNECTION_STRING is supplied. It exercises a real
// Functions host and checks application values after each durable transport
// boundary has persisted and replayed them.
func TestDurableBlobOffloadingRoundTrip(t *testing.T) {
	if os.Getenv(durableBlobConnectionStringEnv) == "" {
		t.Skipf("set %s to opt in to Azure Blob payload offloading", durableBlobConnectionStringEnv)
	}
	app := startDurableBlobApp(t)
	expectedInput := blobPayloadValue{Value: strings.Repeat("orchestration-input-", 17000)}
	expectedEvent := blobPayloadValue{Value: strings.Repeat("external-event-", 23000)}

	instanceID := app.scheduleOrchestration("/api/blob-offloading/start", expectedInput)
	app.waitForStatus(instanceID, "large custom status and event wait", durableCompleteTimeout, func(status orchestrationStatus) bool {
		var customStatus blobPayloadValue
		return json.Unmarshal(status.CustomStatus, &customStatus) == nil && customStatus == expectedInput
	})
	if status := app.status(instanceID); status.RuntimeStatus != "Running" {
		t.Fatalf("expected orchestration to wait for the external event, got %q", status.RuntimeStatus)
	}

	if status, body := app.post("/api/blob-offloading/"+instanceID+"/event", expectedEvent); status != http.StatusAccepted {
		t.Fatalf("raise large external event: expected status 202, got %d: %s", status, body)
	}
	status := app.waitForCompletion(instanceID)

	var gotInput, gotCustomStatus blobPayloadValue
	if err := json.Unmarshal(status.Input, &gotInput); err != nil {
		t.Fatalf("decode orchestration input %s: %v", status.Input, err)
	}
	if gotInput != expectedInput {
		t.Fatalf("orchestration input did not round-trip exactly")
	}
	if err := json.Unmarshal(status.CustomStatus, &gotCustomStatus); err != nil {
		t.Fatalf("decode custom status %s: %v", status.CustomStatus, err)
	}
	if gotCustomStatus != expectedInput {
		t.Fatalf("custom status did not round-trip exactly")
	}

	var result blobPayloadRoundTrip
	if err := json.Unmarshal(status.Output, &result); err != nil {
		t.Fatalf("decode round-trip output %s: %v", status.Output, err)
	}
	for name, got := range map[string]blobPayloadValue{
		"input":                          result.Input,
		"activity input/output":          result.Activity,
		"sub-orchestration input/output": result.SubOrchestration,
		"external event":                 result.Event,
		"entity state/result":            result.Entity,
	} {
		want := expectedInput
		if name == "external event" || name == "entity state/result" {
			want = expectedEvent
		}
		if got != want {
			t.Errorf("%s did not round-trip exactly (got %d bytes, want %d bytes)",
				name, len(got.Value), len(want.Value))
		}
	}
	if result.InlineString != "small inline string" {
		t.Errorf("inline string activity result = %q, want exact string value", result.InlineString)
	}
	if result.InlineObject.Value != "small inline object" {
		t.Errorf("inline object activity result = %#v, want exact object value", result.InlineObject)
	}
	if !result.Continued {
		t.Error("orchestration did not complete after ContinueAsNew")
	}

	statsResp, err := http.Get(app.baseURL + "/api/blob-offloading/tokens")
	if err != nil {
		t.Fatalf("GET stored payload token stats: %v", err)
	}
	defer statsResp.Body.Close()
	statsBody := readAll(t, statsResp.Body)
	if statsResp.StatusCode != http.StatusOK {
		t.Fatalf("GET stored payload token stats: expected status 200, got %d: %s", statsResp.StatusCode, statsBody)
	}
	var stats struct {
		TokenCount       int `json:"tokenCount"`
		NativeTokenCount int `json:"nativeTokenCount"`
	}
	if err := json.Unmarshal(statsBody, &stats); err != nil {
		t.Fatalf("decode stored payload token stats %s: %v", statsBody, err)
	}
	if stats.TokenCount < 8 || stats.NativeTokenCount != stats.TokenCount {
		t.Fatalf("Azure Blob store persisted %d tokens, %d in its native token format; want at least 8 native tokens",
			stats.TokenCount, stats.NativeTokenCount)
	}
}

func startDurableBlobApp(t *testing.T) *durableApp {
	t.Helper()
	if baseURL := os.Getenv(durableBaseURLEnv); baseURL != "" {
		return &durableApp{t: t, baseURL: strings.TrimSuffix(baseURL, "/")}
	}

	connectionString := os.Getenv(durableBlobConnectionStringEnv)
	// This test explicitly opts its local host into the supplied Azurite or
	// Azure account for durable state as well as the payload store. The worker
	// middleware itself only enables offloading from DURABLE_TEST_BLOB_CONNECTION_STRING.
	host, _ := startDurableHost(t, map[string]string{
		"AzureWebJobsStorage":          connectionString,
		durableBlobConnectionStringEnv: connectionString,
	})
	return &durableApp{t: t, baseURL: host.URL()}
}
