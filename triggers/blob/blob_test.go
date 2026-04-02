package blobtrigger

import (
	"context"
	"os"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/azure/azure-functions-golang-worker/sdk"
)

func TestInit_RegistersFactory(t *testing.T) {
	// The init() function should have registered a factory for "blobTrigger"
	factory, ok := sdk.GetClientFactory("blobTrigger")
	if !ok {
		t.Fatal("expected blob trigger factory to be registered via init()")
	}
	if factory == nil {
		t.Fatal("expected non-nil factory")
	}
}

func TestCreateServiceClient_ConnectionString(t *testing.T) {
	os.Setenv("TestStorage", "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;")
	defer os.Unsetenv("TestStorage")

	client, err := createServiceClient("TestStorage")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestCreateServiceClient_MissingEnvVar(t *testing.T) {
	_, err := createServiceClient("NONEXISTENT_VAR_12345")
	if err == nil {
		t.Fatal("expected error for missing env var")
	}
}

func TestGetOrCreateServiceClient_Caching(t *testing.T) {
	os.Setenv("CacheTestStorage", "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;")
	defer os.Unsetenv("CacheTestStorage")

	client1, err := GetOrCreateServiceClient("CacheTestStorage")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	client2, err := GetOrCreateServiceClient("CacheTestStorage")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if client1 != client2 {
		t.Error("expected same cached client instance")
	}
}

func TestCreateBlobClient_ValidPath(t *testing.T) {
	os.Setenv("BlobTestStorage", "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;")
	defer os.Unsetenv("BlobTestStorage")

	client, err := CreateBlobClient("BlobTestStorage", "mycontainer/myblob.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil blob client")
	}
}

func TestCreateBlobClient_InvalidPath(t *testing.T) {
	os.Setenv("BlobTestStorage2", "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;")
	defer os.Unsetenv("BlobTestStorage2")

	_, err := CreateBlobClient("BlobTestStorage2", "nocontainer")
	if err == nil {
		t.Fatal("expected error for invalid blob path")
	}
}

func newTestApp() *sdk.App {
	return sdk.FunctionApp()
}

func TestRegister(t *testing.T) {
	app := newTestApp()
	handler := func(ctx context.Context, client *blob.Client) error {
		return nil
	}

	rf := Register(app, "blobFunc", handler,
		WithPath("container/{name}"),
		WithBlobConnection("AzureWebJobsStorage"),
	)
	if rf == nil {
		t.Fatal("expected non-nil RegisteredFunction")
	}

	count := 0
	app.GetRegisteredFunctions().Range(func(key, value any) bool {
		count++
		return true
	})

	if count != 1 {
		t.Errorf("expected 1 registered function, got %d", count)
	}
}
