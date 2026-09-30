package worker

import (
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

func TestWorkerMetadataCache(t *testing.T) {
	for _, available := range []bool{false, true} {
		name := "unavailable"
		if available {
			name = "available"
		}
		t.Run(name, func(t *testing.T) {
			var buildReads, sizeReads atomic.Int32
			provider := newWorkerMetadataProvider(func() (*debug.BuildInfo, bool) {
				buildReads.Add(1)
				return &debug.BuildInfo{Deps: []*debug.Module{{Path: sdkModulePath, Version: "v1.2.3"}}}, available
			}, func() string {
				sizeReads.Add(1)
				return "123456"
			})
			if buildReads.Load() != 0 || sizeReads.Load() != 0 {
				t.Fatal("metadata collection must be lazy")
			}
			results := make(chan *pb.WorkerMetadata, 64)
			var wg sync.WaitGroup
			for i := 0; i < cap(results); i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					md := provider()
					md.RuntimeName = "changed"
					md.CustomProperties[MetaAppDependencies] = "changed"
					md.CustomProperties[MetaAppBinarySizeBytes] = "changed"
					results <- md
				}()
			}
			wg.Wait()
			close(results)
			seen := make(map[*pb.WorkerMetadata]bool)
			for md := range results {
				if seen[md] {
					t.Fatal("callers share a mutable metadata object")
				}
				seen[md] = true
			}
			md := provider()
			inventory := readInventory(t, md)
			wantStatus := "unavailable"
			if available {
				wantStatus = "available"
				if md.WorkerVersion != "v1.2.3" {
					t.Fatalf("SDK version = %q", md.WorkerVersion)
				}
			}
			if md.RuntimeName != "go" || inventory.Status != wantStatus || md.CustomProperties[MetaAppBinarySizeBytes] != "123456" {
				t.Fatalf("cached metadata mutated or build-info availability ignored: %v", md)
			}
			if buildReads.Load() != 1 || sizeReads.Load() != 1 {
				t.Fatalf("expected one read each, build=%d size=%d", buildReads.Load(), sizeReads.Load())
			}
		})
	}
}

func TestWorkerMetadataCacheUnavailableSize(t *testing.T) {
	var calls int
	provider := newWorkerMetadataProvider(func() (*debug.BuildInfo, bool) { return nil, false }, func() string {
		calls++
		return ""
	})
	for i := 0; i < 3; i++ {
		md := provider()
		if size, ok := md.CustomProperties[MetaAppBinarySizeBytes]; !ok || size != "" {
			t.Fatalf("unavailable size should be present and empty: %v", md)
		}
	}
	if calls != 1 {
		t.Fatalf("unavailable snapshot was retried %d times", calls)
	}
}

func TestBuildWorkerMetadataIndependentResponses(t *testing.T) {
	first := buildWorkerMetadata()
	second := buildWorkerMetadata()
	first.RuntimeName = "changed"
	first.CustomProperties[MetaAppDependencies] = "changed"
	first.CustomProperties[MetaAppBinarySizeBytes] = "changed"
	if first == second || second.RuntimeName != "go" || second.CustomProperties[MetaAppDependencies] == "changed" || second.CustomProperties[MetaAppBinarySizeBytes] == "changed" {
		t.Fatal("process metadata responses share mutable state")
	}
}
