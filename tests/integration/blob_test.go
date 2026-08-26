package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

const azuriteConnStr = "DefaultEndpointsProtocol=http;" +
	"AccountName=devstoreaccount1;" +
	"AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
	"BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;" +
	"QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;" +
	"TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"

var blobEnv = map[string]string{
	"AzureWebJobsStorage": azuriteConnStr,
}

func TestBlobTriggerFires(t *testing.T) {
	requireAzurite(t)
	ctx := context.Background()

	// Create the container before starting the host
	client, err := azblob.NewClientFromConnectionString(azuriteConnStr, nil)
	if err != nil {
		t.Fatalf("failed to create blob client: %v", err)
	}
	_, err = client.CreateContainer(ctx, "test-container", nil)
	if err != nil && !isContainerAlreadyExists(err) {
		t.Fatalf("failed to create container: %v", err)
	}

	host := startTestDataHost(t, "blobTrigger", blobEnv, 60*time.Second)

	// Upload a unique blob after the host is initialized
	blobName := fmt.Sprintf("test-%d.txt", time.Now().UnixNano())
	blobContent := "Hello from blob trigger integration test!"
	_, err = client.UploadBuffer(ctx, "test-container", blobName, []byte(blobContent), nil)
	if err != nil {
		t.Fatalf("failed to upload blob: %v", err)
	}

	// Blob trigger uses polling — can take up to 60 seconds
	assertHostLogContains(t, host, "blob trigger executed", 90*time.Second)
	assertHostLogContains(t, host, "Executing 'Functions.blobTrigger'", 5*time.Second)

	// Verify the ClientFactory created a real *blob.Client with a valid endpoint
	logBytes, err := os.ReadFile(host.LogPath())
	if err != nil {
		t.Fatalf("read host log: %v", err)
	}
	if !strings.Contains(string(logBytes), "url=http") {
		t.Fatal("Expected blob client URL to start with 'http', indicating ClientFactory created a real client")
	}
}

func isContainerAlreadyExists(err error) bool {
	return err != nil && contains(err.Error(), "ContainerAlreadyExists")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
