package integration

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/azure/azure-functions-golang-worker/tests/integration/internal/testhost"
)

func TestGenericTriggerQueues(t *testing.T) {
	requireAzurite(t)
	service, err := azqueue.NewServiceClientFromConnectionString(azuriteConnStr, nil)
	if err != nil {
		t.Fatalf("create queue service client: %v", err)
	}
	unique := strings.ToLower(rand.Text()[:20])
	cases := []struct {
		function string
		setting  string
		suffix   string
		body     string
		log      string
	}{
		{"RawQueue", "GenericRawQueue", "raw", "raw-" + unique + ` {"literal":true}`, ""},
		{"JSONQueue", "GenericJSONQueue", "json", fmt.Sprintf(`{"id":"json-%s","quantity":7}`, unique), "typed order trigger_type=queueTrigger id=json-" + unique + " quantity=7"},
		{"TypedQueue", "GenericTypedQueue", "typed", fmt.Sprintf(`{"id":"typed-%s","quantity":11}`, unique), "typed order trigger_type=queueTrigger id=typed-" + unique + " quantity=11"},
		{"MetadataQueue", "GenericMetadataQueue", "metadata", "metadata-" + unique, "order metadata trigger_type=queueTrigger body=metadata-" + unique},
	}
	cases[0].log = "raw order trigger_type=queueTrigger body=" + strconv.Quote(cases[0].body)
	env := make(map[string]string, len(cases))
	for _, tc := range cases {
		queueName := "generic-" + unique + "-" + tc.suffix
		env[tc.setting] = queueName
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		_, err := service.CreateQueue(ctx, queueName, nil)
		cancel()
		if err != nil {
			t.Fatalf("create queue %s: %v", queueName, err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, err := service.DeleteQueue(ctx, queueName, nil); err != nil {
				t.Errorf("delete test queue %s: %v", queueName, err)
			}
		})
	}
	// Register host cleanup after queue cleanup so the host stops before deletion.
	host := startGenericTriggerHost(t, "queues", env)
	for _, tc := range cases {
		t.Run(tc.function, func(t *testing.T) {
			queue := service.NewQueueClient(env[tc.setting])
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			message, err := queue.EnqueueMessage(ctx, base64.StdEncoding.EncodeToString([]byte(tc.body)), nil)
			if err != nil {
				t.Fatalf("enqueue %s message: %v", tc.function, err)
			}
			assertHostLogContains(t, host, tc.log, 30*time.Second)
			assertHostLogContains(t, host, "generic middleware trigger_type=queueTrigger function="+tc.function+" trigger=queueTrigger", 5*time.Second)
			assertHostLogContains(t, host, "Executed 'Functions."+tc.function+"' (Succeeded", 10*time.Second)
			if tc.function == "MetadataQueue" {
				if len(message.Messages) != 1 || message.Messages[0] == nil || message.Messages[0].MessageID == nil {
					t.Fatal("enqueued metadata message has no ID")
				}
				assertGenericQueueMetadata(t, host, tc.log, *message.Messages[0].MessageID)
			}
		})
	}
}

func assertGenericQueueMetadata(t *testing.T, host testhost.Host, marker, messageID string) {
	t.Helper()
	data, err := os.ReadFile(host.LogPath())
	if err != nil {
		t.Fatalf("read metadata log: %v", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.Contains(line, marker+" metadata=") {
			continue
		}
		_, encoded, _ := strings.Cut(line, " metadata=")
		value, err := strconv.Unquote(strings.TrimSpace(encoded))
		if err != nil {
			t.Fatalf("unquote metadata JSON: %v", err)
		}
		var metadata struct {
			ID              string `json:"Id"`
			DequeueCount    int64
			InsertionTime   string
			ExpirationTime  string
			NextVisibleTime string
			PopReceipt      string
		}
		if err := json.Unmarshal([]byte(value), &metadata); err != nil {
			t.Fatalf("decode metadata JSON: %v", err)
		}
		if metadata.ID != messageID || metadata.DequeueCount != 1 {
			t.Errorf("metadata Id=%q DequeueCount=%d, want Id=%q DequeueCount=1", metadata.ID, metadata.DequeueCount, messageID)
		}
		if metadata.PopReceipt == "" {
			t.Error("metadata PopReceipt is empty")
		}
		for name, value := range map[string]string{
			"InsertionTime": metadata.InsertionTime, "ExpirationTime": metadata.ExpirationTime, "NextVisibleTime": metadata.NextVisibleTime,
		} {
			if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
				t.Errorf("metadata %s is not an RFC3339 timestamp: %v", name, err)
			}
		}
		return
	}
	t.Fatal("metadata JSON log for the unique message is missing")
}

func TestGenericTriggerMCP(t *testing.T) {
	requireAzurite(t)
	host := startGenericTriggerHost(t, "mcp", nil)
	client := mcpTestClient{
		endpoint: host.URL() + "/runtime/webhooks/mcp",
		client:   &http.Client{Timeout: 30 * time.Second},
	}
	t.Cleanup(client.client.CloseIdleConnections)
	initialized := client.request(t, 1, "initialize", map[string]any{
		"protocolVersion": "2025-03-26",
		"clientInfo":      map[string]string{"name": "go-worker-test", "version": "1.0"},
		"capabilities":    map[string]any{},
	})
	var initialization struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	if err := json.Unmarshal(initialized, &initialization); err != nil {
		t.Fatalf("decode initialize result: %v", err)
	}
	if initialization.ProtocolVersion != "2025-03-26" {
		t.Fatalf("MCP protocol version = %q, want 2025-03-26", initialization.ProtocolVersion)
	}
	client.request(t, 0, "notifications/initialized", map[string]any{})
	listed := client.request(t, 2, "tools/list", map[string]any{})
	var listing struct {
		Tools []struct {
			Name        string `json:"name"`
			InputSchema struct {
				Type       string `json:"type"`
				Properties map[string]struct {
					Type string `json:"type"`
				} `json:"properties"`
				Required []string `json:"required"`
			} `json:"inputSchema"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(listed, &listing); err != nil {
		t.Fatalf("decode tools/list result: %v", err)
	}
	echoCount := 0
	for _, tool := range listing.Tools {
		if tool.Name != "echo" {
			continue
		}
		echoCount++
		if tool.InputSchema.Type != "object" || tool.InputSchema.Properties["text"].Type != "string" || !slices.Contains(tool.InputSchema.Required, "text") {
			t.Fatalf("echo schema must describe an object with required string text: %+v", tool.InputSchema)
		}
	}
	if echoCount != 1 {
		t.Fatalf("tools/list contains %d echo tools, want 1", echoCount)
	}
	text := "generic-echo-" + strings.ToLower(rand.Text())
	called := client.request(t, 3, "tools/call", map[string]any{
		"name": "echo", "arguments": map[string]string{"text": text},
	})
	var result struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(called, &result); err != nil {
		t.Fatalf("decode tools/call result: %v", err)
	}
	if result.IsError || len(result.Content) != 1 || result.Content[0].Type != "text" || result.Content[0].Text != text {
		t.Fatalf("tools/call result = %+v, want echoed text %q", result, text)
	}
	assertHostLogContains(t, host, "echo tool trigger_type=mcpToolTrigger text="+text, 5*time.Second)
	assertHostLogContains(t, host, "generic middleware trigger_type=mcpToolTrigger function=EchoTool trigger=mcpToolTrigger", 5*time.Second)
	assertHostLogContains(t, host, "Executed 'Functions.EchoTool' (Succeeded", 10*time.Second)
}

func startGenericTriggerHost(t *testing.T, scenario string, settings map[string]string) testhost.Host {
	t.Helper()
	appDir := t.TempDir()
	binDir := filepath.Join(appDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("create sample binary directory: %v", err)
	}
	appName := "app"
	if runtime.GOOS == "windows" {
		appName += ".exe"
	}
	buildCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(buildCtx, "go", "build", "-o", filepath.Join(binDir, appName), "./samples/genericTriggers")
	cmd.Dir = repoRoot()
	cmd.Env = append(os.Environ(), "GOWORK=off")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build genericTriggers sample: %v\n%s", err, output)
	}
	hostJSON, err := os.ReadFile(filepath.Join(samplesDir(), "genericTriggers", "host.json"))
	if err != nil {
		t.Fatalf("read genericTriggers host configuration: %v", err)
	}
	if err := os.WriteFile(filepath.Join(appDir, "host.json"), hostJSON, 0o600); err != nil {
		t.Fatalf("copy genericTriggers host configuration: %v", err)
	}
	env := make(map[string]string, len(settings)+4)
	for key, value := range settings {
		env[key] = value
	}
	hostID := "generic-" + strings.ToLower(rand.Text()[:20])
	env["GOWORK"] = "off"
	env["AzureWebJobsStorage"] = azuriteConnStr
	env["AzureFunctionsWebHost__hostid"] = hostID
	env["GENERIC_SCENARIO"] = scenario
	host, err := testhost.Start(context.Background(), testhost.Config{
		SampleDir: appDir, FuncExe: funcExe(), NoBuild: true, Environment: env,
		ArtifactDir: filepath.Join("artifacts", t.Name(), hostID), InitTimeout: 90 * time.Second,
	})
	if err != nil {
		t.Fatalf("start genericTriggers %s host: %v", scenario, err)
	}
	t.Logf("genericTriggers %s host log: %s", scenario, host.LogPath())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := host.Stop(ctx); err != nil {
			t.Errorf("stop genericTriggers host: %v", err)
		}
	})
	return host
}
