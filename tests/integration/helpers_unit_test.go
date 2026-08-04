package integration

import "testing"

func TestWithNativeWorkerEnvironment(t *testing.T) {
	input := map[string]string{
		"AzureWebJobsStorage":      "UseDevelopmentStorage=true",
		"FUNCTIONS_WORKER_RUNTIME": "golang",
	}

	got := withNativeWorkerEnvironment(input)

	if got["FUNCTIONS_WORKER_RUNTIME"] != "native" {
		t.Fatalf("FUNCTIONS_WORKER_RUNTIME = %q, want native", got["FUNCTIONS_WORKER_RUNTIME"])
	}
	if got["FUNCTIONS_CLI_NATIVE_LANGUAGE"] != "go" {
		t.Fatalf("FUNCTIONS_CLI_NATIVE_LANGUAGE = %q, want go", got["FUNCTIONS_CLI_NATIVE_LANGUAGE"])
	}
	if got["AzureWebJobsStorage"] != input["AzureWebJobsStorage"] {
		t.Fatalf("AzureWebJobsStorage = %q, want %q", got["AzureWebJobsStorage"], input["AzureWebJobsStorage"])
	}
	if input["FUNCTIONS_WORKER_RUNTIME"] != "golang" {
		t.Fatal("withNativeWorkerEnvironment mutated its input")
	}
}
