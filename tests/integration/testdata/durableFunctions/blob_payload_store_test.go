package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/payload"
)

func TestAzureBlobStoreTokenBoundaryRoundTrips(t *testing.T) {
	connectionString := os.Getenv(blobPayloadConnectionStringEnv)
	if connectionString == "" {
		t.Skipf("set %s to run Azure Blob store checks against Azurite", blobPayloadConnectionStringEnv)
	}

	store, container, err := newBoundaryPayloadStore(connectionString)
	if err != nil {
		t.Fatal(err)
	}
	threshold, maxPayload := store.LargePayloadDefaults()
	if threshold != 256*1024 || maxPayload != 10*1024*1024 {
		t.Fatalf("Azure Blob defaults are threshold=%d cap=%d, want 262144 and 10485760",
			threshold, maxPayload)
	}

	ctx := context.Background()
	for _, size := range []int{threshold - 1, threshold, threshold + 1, maxPayload} {
		t.Run(fmt.Sprintf("roundtrip-%d-bytes", size), func(t *testing.T) {
			serialized := serializedJSONOfSize(size)
			token, err := store.StoreToken(ctx, serialized)
			if err != nil {
				t.Fatalf("StoreToken(%d bytes): %v", size, err)
			}
			if !store.IsLargePayloadToken(token) {
				t.Fatalf("StoreToken(%d bytes) returned a non-native token", size)
			}
			resolved, err := store.ResolveToken(ctx, token)
			if err != nil {
				t.Fatalf("ResolveToken(%d bytes): %v", size, err)
			}
			if !bytes.Equal(resolved, serialized) {
				t.Fatalf("ResolveToken(%d bytes) returned different serialized bytes", size)
			}
		})
	}

	if _, err := store.StoreToken(ctx, serializedJSONOfSize(maxPayload+1)); !errors.Is(err, api.ErrLargePayloadTooLarge) {
		t.Fatalf("StoreToken(%d bytes) error = %v, want ErrLargePayloadTooLarge", maxPayload+1, err)
	}

	t.Run("missing-blob", func(t *testing.T) {
		token, err := store.StoreToken(ctx, []byte(`{"v":"missing"}`))
		if err != nil {
			t.Fatalf("StoreToken for missing-blob case: %v", err)
		}
		client, err := azblob.NewClientFromConnectionString(connectionString, nil)
		if err != nil {
			t.Fatalf("create Azure Blob client: %v", err)
		}
		name, err := blobNameFromToken(token)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.DeleteBlob(ctx, container, name, nil); err != nil {
			t.Fatalf("delete test payload blob: %v", err)
		}
		if _, err := store.ResolveToken(ctx, token); !errors.Is(err, api.ErrLargePayloadReference) {
			t.Fatalf("ResolveToken for missing blob error = %v, want ErrLargePayloadReference", err)
		}
	})

	t.Run("modified-content-integrity", func(t *testing.T) {
		original := []byte(`{"v":"original"}`)
		modified := []byte(`{"v":"modified"}`)
		token, err := store.StoreToken(ctx, original)
		if err != nil {
			t.Fatalf("StoreToken for integrity case: %v", err)
		}
		client, err := azblob.NewClientFromConnectionString(connectionString, nil)
		if err != nil {
			t.Fatalf("create Azure Blob client: %v", err)
		}
		name, err := blobNameFromToken(token)
		if err != nil {
			t.Fatal(err)
		}
		size := strconv.Itoa(len(original))
		digest := sha256.Sum256(original)
		hash := hex.EncodeToString(digest[:])
		metadata := map[string]*string{
			"durabletask_size":   &size,
			"durabletask_sha256": &hash,
		}
		if _, err := client.UploadBuffer(ctx, container, name, modified,
			&azblob.UploadBufferOptions{Metadata: metadata}); err != nil {
			t.Fatalf("overwrite test payload blob: %v", err)
		}
		if _, err := store.ResolveToken(ctx, token); !errors.Is(err, api.ErrLargePayloadIntegrity) {
			t.Fatalf("ResolveToken after content modification error = %v, want ErrLargePayloadIntegrity", err)
		}
	})
}

func newBoundaryPayloadStore(connectionString string) (*payload.AzureBlobStore, string, error) {
	container := fmt.Sprintf("payload-boundary-%d", time.Now().UnixNano())
	store, err := payload.NewAzureBlobStore(payload.AzureBlobStoreOptions{
		ConnectionString:  connectionString,
		Container:         container,
		AllowInsecureHTTP: true,
	})
	return store, container, err
}

func serializedJSONOfSize(size int) []byte {
	const prefix, suffix = `{"v":"`, `"}`
	return []byte(prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix)
}

func blobNameFromToken(token string) (string, error) {
	location := strings.TrimPrefix(token, api.AzureBlobPayloadReferencePrefixV2)
	parsed, err := url.Parse(location)
	if err != nil {
		return "", fmt.Errorf("parse blob token location: %w", err)
	}
	name := path.Base(parsed.Path)
	if name == "." || name == "/" || name == "" {
		return "", fmt.Errorf("blob token has no blob name")
	}
	return name, nil
}
