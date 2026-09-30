package worker

import (
	"runtime"
	"runtime/debug"
	"sync"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
	"google.golang.org/protobuf/proto"
)

// sdkModulePath is the canonical Go module path of this worker SDK. Used
// to locate the SDK's BuildInfo dependency entry inside the user's app
// when reporting WorkerVersion.
const sdkModulePath = "github.com/azure/azure-functions-golang-worker"

// Custom-property keys reported in WorkerInitResponse.WorkerMetadata.
// These are documented stable identifiers that telemetry consumers can
// query in Kusto.
const (
	// MetaSDKReplaced is "true" when the user's go.mod has a `replace`
	// directive pointing the SDK at a local path or alternate version.
	// Useful for narrowing investigations: telemetry from a replaced SDK
	// may not match the official version.
	MetaSDKReplaced = "sdk_replaced"

	// MetaSDKReplacePath is the `replace` directive's target path (e.g.
	// "../azure-functions-golang-worker") or alternate module path. Empty
	// when sdk_replaced is "false".
	MetaSDKReplacePath = "sdk_replace_path"

	// MetaAppBuiltDirty is "true" when the user's app was built with
	// uncommitted local changes (vcs.modified). Indicates a developer
	// build rather than a CI-built release artifact.
	MetaAppBuiltDirty = "app_built_dirty"

	// MetaAppVCSRevision is the git commit SHA the app was built from
	// (vcs.revision). Empty when the build did not include VCS info
	// (e.g. -buildvcs=false or builds outside a VCS root).
	MetaAppVCSRevision = "app_vcs_revision"

	// MetaAppDependencies contains a versioned JSON inventory of the
	// dependency modules embedded in the application binary. It includes private
	// and transitive module identities, but not local replacement directories.
	MetaAppDependencies = "app_dependencies"

	// MetaAppBinarySizeBytes is the executable file's logical length in bytes,
	// captured at first metadata collection. Empty means unavailable, not zero.
	MetaAppBinarySizeBytes = "app_binary_size_bytes"
)

var processWorkerMetadata = newWorkerMetadataProvider(debug.ReadBuildInfo, appBinarySize)

// newWorkerMetadataProvider retains a private, process-lifetime snapshot. Each
// caller owns its returned protobuf and map; only immutable strings are shared.
// The readers are injected here so tests need not reset package-global caches.
func newWorkerMetadataProvider(readBuildInfo func() (*debug.BuildInfo, bool), readBinarySize func() string) func() *pb.WorkerMetadata {
	template := sync.OnceValue(func() *pb.WorkerMetadata {
		bi, ok := readBuildInfo()
		if !ok {
			bi = nil
		}
		md := buildWorkerMetadataFromBuildInfo(bi)
		// File size is independent of embedded build-info availability.
		md.CustomProperties[MetaAppBinarySizeBytes] = readBinarySize()
		return md
	})
	return func() *pb.WorkerMetadata {
		return proto.Clone(template()).(*pb.WorkerMetadata)
	}
}

// buildWorkerMetadata constructs the WorkerMetadata reported in
// WorkerInitResponse and FunctionEnvironmentReloadResponse. Custom properties
// are always present, including an unavailable inventory when build information
// cannot be read.
//
// The SDK version is read from the user app's BuildInfo dependency tree;
// it is "(devel)" when the app is built outside a release-tag commit, or
// "(replaced)" when a `replace` directive points the SDK at a
// versionless local path.
func buildWorkerMetadata() *pb.WorkerMetadata {
	return processWorkerMetadata()
}

func buildWorkerMetadataFromBuildInfo(bi *debug.BuildInfo) *pb.WorkerMetadata {
	md := &pb.WorkerMetadata{
		RuntimeName:    "go",
		RuntimeVersion: runtime.Version(),
		WorkerBitness:  runtime.GOOS + "/" + runtime.GOARCH,
		CustomProperties: map[string]string{
			MetaSDKReplaced:        "false",
			MetaSDKReplacePath:     "",
			MetaAppBuiltDirty:      "false",
			MetaAppVCSRevision:     "",
			MetaAppDependencies:    buildDependencyInventory(bi),
			MetaAppBinarySizeBytes: "",
		},
	}

	if bi == nil {
		return md
	}

	for _, dep := range bi.Deps {
		if dep == nil {
			continue
		}
		if dep.Path != sdkModulePath {
			continue
		}
		md.WorkerVersion = dep.Version
		if dep.Replace != nil {
			md.CustomProperties[MetaSDKReplaced] = "true"
			md.CustomProperties[MetaSDKReplacePath] = dep.Replace.Path
			if dep.Replace.Version != "" {
				md.WorkerVersion = dep.Replace.Version
			} else {
				md.WorkerVersion = "(replaced)"
			}
		}
		break
	}
	if md.WorkerVersion == "" {
		md.WorkerVersion = "(devel)"
	}

	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			md.CustomProperties[MetaAppVCSRevision] = s.Value
		case "vcs.modified":
			if s.Value == "true" {
				md.CustomProperties[MetaAppBuiltDirty] = "true"
			}
		}
	}

	return md
}
