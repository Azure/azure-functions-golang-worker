package worker

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"
	"testing"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
	"google.golang.org/protobuf/proto"
)

// Decode into independent wire types so changes to the producer's schema do not
// silently change the expectations in these tests.
type inventoryWire struct {
	SchemaVersion int          `json:"schema_version"`
	Status        string       `json:"status"`
	Total         int          `json:"total"`
	Reported      int          `json:"reported"`
	Truncated     bool         `json:"truncated"`
	Modules       []moduleWire `json:"modules"`
}

type moduleWire struct {
	Path        string           `json:"path"`
	Version     string           `json:"version,omitempty"`
	Replacement *replacementWire `json:"replacement,omitempty"`
}

type replacementWire struct {
	Path    string `json:"path,omitempty"`
	Version string `json:"version,omitempty"`
}

func readInventory(t *testing.T, md *pb.WorkerMetadata) inventoryWire {
	t.Helper()
	raw, ok := md.CustomProperties["app_dependencies"]
	if !ok {
		t.Fatal("WorkerMetadata is missing app_dependencies")
	}
	var result inventoryWire
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&result); err != nil {
		t.Fatalf("invalid inventory JSON: %v", err)
	}
	if result.SchemaVersion != 1 || result.Modules == nil {
		t.Fatalf("invalid inventory envelope: %+v", result)
	}
	if result.Reported != len(result.Modules) || result.Total != result.Reported || result.Truncated {
		t.Fatalf("inconsistent inventory counts: %+v", result)
	}
	return result
}

func TestDependencyMetadataContents(t *testing.T) {
	bi := &debug.BuildInfo{
		Main:     debug.Module{Path: "private.example/customer/app", Version: "v99.0.0"},
		Path:     "private.example/customer/app/cmd/server",
		Settings: []debug.BuildSetting{{Key: "-ldflags", Value: "secret-build-setting"}},
		Deps: []*debug.Module{
			{Path: "private.example/team/client", Version: "v2.3.4", Sum: "secret-checksum"},
			{Path: "example.org/replaced", Version: "v0.1.0", Replace: &debug.Module{Path: "private.example/fork", Version: "v1.2.3"}},
			{Path: "example.org/local", Version: "v7.0.0", Replace: &debug.Module{Path: "C:/private/source"}},
			{Path: sdkModulePath, Version: "v0.7.0-preview"},
			{Path: "example.org/unversioned", Version: "(devel)"},
			{Path: "example.org/workspace"},
			nil,
			{},
			{Path: "private.example/team/client", Version: "v2.3.4"},
		},
	}
	md := buildWorkerMetadataFromBuildInfo(bi)
	got := readInventory(t, md)
	want := inventoryWire{
		SchemaVersion: 1, Status: "available", Total: 6, Reported: 6,
		Modules: []moduleWire{
			{Path: "example.org/local", Replacement: &replacementWire{}},
			{Path: "example.org/replaced", Version: "v0.1.0", Replacement: &replacementWire{Path: "private.example/fork", Version: "v1.2.3"}},
			{Path: "example.org/unversioned", Version: "(devel)"},
			{Path: "example.org/workspace"},
			{Path: sdkModulePath, Version: "v0.7.0-preview"},
			{Path: "private.example/team/client", Version: "v2.3.4"},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("inventory = %+v, want %+v", got, want)
	}
	for _, excluded := range []string{bi.Main.Path, "secret-build-setting", "secret-checksum", "C:/private/source", "v7.0.0"} {
		if strings.Contains(md.CustomProperties["app_dependencies"], excluded) {
			t.Errorf("inventory contains excluded value %q", excluded)
		}
	}
	if md.WorkerVersion != "v0.7.0-preview" {
		t.Fatalf("existing SDK version changed: %q", md.WorkerVersion)
	}
}

func TestDependencyMetadataUnavailableAndEmpty(t *testing.T) {
	for _, tc := range []struct {
		name   string
		info   *debug.BuildInfo
		status string
	}{
		{"unavailable", nil, "unavailable"},
		{"empty", &debug.BuildInfo{}, "available"},
		{"invalid entries", &debug.BuildInfo{Deps: []*debug.Module{nil, {}}}, "available"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			md := buildWorkerMetadataFromBuildInfo(tc.info)
			got := readInventory(t, md)
			if got.Status != tc.status || got.Total != 0 || got.Reported != 0 || got.Truncated {
				t.Fatalf("unexpected empty inventory: %+v", got)
			}
			if md.CustomProperties[MetaSDKReplaced] != "false" || md.CustomProperties[MetaSDKReplacePath] != "" || md.CustomProperties[MetaAppBuiltDirty] != "false" || md.CustomProperties[MetaAppVCSRevision] != "" {
				t.Fatalf("existing defaults changed: %v", md.CustomProperties)
			}
		})
	}
}

func TestDependencyMetadataDeterministicAndComplete(t *testing.T) {
	bi := &debug.BuildInfo{}
	for i := 0; i < 1000; i++ {
		bi.Deps = append(bi.Deps, &debug.Module{Path: fmt.Sprintf("private.example/module%04d", i), Version: "v1.0.0"})
	}
	md := buildWorkerMetadataFromBuildInfo(bi)
	got := readInventory(t, md)
	if got.Total != 1000 || got.Reported != 1000 || got.Truncated {
		t.Fatalf("expected all 1000 modules: %+v", got)
	}
	if len(md.CustomProperties[MetaAppDependencies]) <= 8*1024 {
		t.Fatal("fixture must exceed the former 8 KiB limit")
	}
	for i, module := range got.Modules {
		want := moduleWire{Path: fmt.Sprintf("private.example/module%04d", i), Version: "v1.0.0"}
		if !reflect.DeepEqual(module, want) {
			t.Fatalf("module %d = %+v, want %+v", i, module, want)
		}
	}
	// The full inventory must survive serialization of both lifecycle messages.
	// This does not assert that downstream telemetry stores accept the same size.
	for _, message := range []*pb.StreamingMessage{
		{Content: &pb.StreamingMessage_WorkerInitResponse{WorkerInitResponse: &pb.WorkerInitResponse{WorkerMetadata: md}}},
		{Content: &pb.StreamingMessage_FunctionEnvironmentReloadResponse{FunctionEnvironmentReloadResponse: &pb.FunctionEnvironmentReloadResponse{WorkerMetadata: md}}},
	} {
		encoded, err := proto.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		var decoded pb.StreamingMessage
		if err := proto.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(message, &decoded) {
			t.Fatal("large lifecycle metadata changed during protobuf round trip")
		}
	}
	for i, j := 0, len(bi.Deps)-1; i < j; i, j = i+1, j-1 {
		bi.Deps[i], bi.Deps[j] = bi.Deps[j], bi.Deps[i]
	}
	bi.Deps = append(bi.Deps, bi.Deps[0])
	if again := buildWorkerMetadataFromBuildInfo(bi).CustomProperties["app_dependencies"]; again != md.CustomProperties["app_dependencies"] {
		t.Fatal("inventory changes with ordering or identical duplicate records")
	}
}

func TestDependencyMetadataEscapingAndLargeEntry(t *testing.T) {
	for _, path := range []string{strings.Repeat("界", 3000), strings.Repeat("\"\\\n", 2000), strings.Repeat("<>&", 2000)} {
		bi := &debug.BuildInfo{Deps: []*debug.Module{{Path: "a.example/" + path, Version: "v1.0.0"}, {Path: "z.example/small", Version: "v1.0.0"}}}
		md := buildWorkerMetadataFromBuildInfo(bi)
		got := readInventory(t, md)
		want := []moduleWire{{Path: "a.example/" + path, Version: "v1.0.0"}, {Path: "z.example/small", Version: "v1.0.0"}}
		if !reflect.DeepEqual(got.Modules, want) {
			t.Fatal("large or escaped module identity was omitted or altered")
		}
		if len(md.CustomProperties[MetaAppDependencies]) <= 8*1024 {
			t.Fatal("escaped fixture must exceed the former 8 KiB limit")
		}
	}
}

func TestDependencyMetadataIdentityAndReplacement(t *testing.T) {
	// Conflicting versions must not be silently collapsed. Local replacements
	// have no trustworthy effective version, regardless of their requested pin.
	bi := &debug.BuildInfo{Deps: []*debug.Module{
		{Path: "example.org/module", Version: "v1.0.0"},
		{Path: "example.org/module", Version: "v2.0.0"},
		{Path: "example.org/module", Version: "v3.0.0", Replace: &debug.Module{Path: "../local"}},
		{Path: "example.org/module", Version: "v4.0.0", Replace: &debug.Module{Path: "/another/local"}},
		{Path: "example.org/module", Version: "v5.0.0", Replace: &debug.Module{Path: "../devel", Version: "(devel)"}},
	}}
	got := readInventory(t, buildWorkerMetadataFromBuildInfo(bi))
	if got.Total != 3 {
		t.Fatalf("expected distinct version identities and one redacted local identity: %+v", got)
	}
	// BuildInfo is caller-owned; sorting and redaction must not mutate it.
	if bi.Deps[2].Version != "v3.0.0" || bi.Deps[2].Replace.Path != "../local" {
		t.Fatal("input metadata was mutated")
	}
	bi.Deps[2].Replace.Replace = bi.Deps[2]
	readInventory(t, buildWorkerMetadataFromBuildInfo(bi))
}

func TestDependencyMetadataFormerByteBoundary(t *testing.T) {
	// Determine the one-record envelope length independently of the producer.
	wire := inventoryWire{SchemaVersion: 1, Status: "available", Total: 1, Reported: 1,
		Modules: []moduleWire{{Path: "example.org/", Version: "v1.0.0"}}}
	base, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	for _, extra := range []int{-1, 0, 1} {
		path := "example.org/" + strings.Repeat("x", 8*1024-len(base)+extra)
		md := buildWorkerMetadataFromBuildInfo(&debug.BuildInfo{
			Deps: []*debug.Module{{Path: path, Version: "v1.0.0"}},
		})
		got := readInventory(t, md)
		if got.Reported != 1 || got.Truncated || got.Modules[0].Path != path {
			t.Fatalf("boundary+%d: %+v", extra, got)
		}
		if size := len(md.CustomProperties[MetaAppDependencies]); size != 8*1024+extra {
			t.Fatalf("boundary+%d: inventory bytes = %d", extra, size)
		}
	}
}

func TestDependencyMetadataPreservesExistingFields(t *testing.T) {
	for _, replacement := range []*debug.Module{nil, {Path: "../sdk"}, {Path: "../sdk", Version: "(devel)"}, {Path: "example.org/sdk", Version: "v9.0.0"}} {
		bi := &debug.BuildInfo{
			Deps:     []*debug.Module{{Path: sdkModulePath, Version: "v0.7.0-preview", Replace: replacement}},
			Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "revision"}, {Key: "vcs.modified", Value: "true"}},
		}
		md := buildWorkerMetadataFromBuildInfo(bi)
		wantVersion, wantReplaced, wantPath := "v0.7.0-preview", "false", ""
		if replacement != nil {
			wantReplaced, wantPath, wantVersion = "true", replacement.Path, replacement.Version
			if wantVersion == "" {
				wantVersion = "(replaced)"
			}
		}
		if md.WorkerVersion != wantVersion || md.CustomProperties[MetaSDKReplaced] != wantReplaced || md.CustomProperties[MetaSDKReplacePath] != wantPath || md.CustomProperties[MetaAppVCSRevision] != "revision" || md.CustomProperties[MetaAppBuiltDirty] != "true" {
			t.Fatalf("existing metadata changed: %+v", md)
		}
		readInventory(t, md)
	}
}

func TestDependencyMetadataHandshake(t *testing.T) {
	disp := newTestDispatcher("inventory-request")
	init := handleWorkerInitRequest(&pb.WorkerInitRequest{}, "inventory-request", disp).GetWorkerInitResponse()
	reload, err := handleFunctionEnvironmentReloadRequest("inventory-request", &pb.FunctionEnvironmentReloadRequest{})
	if err != nil {
		t.Fatal(err)
	}
	readInventory(t, init.WorkerMetadata)
	readInventory(t, reload.GetFunctionEnvironmentReloadResponse().WorkerMetadata)
	if !reflect.DeepEqual(init.WorkerMetadata, reload.GetFunctionEnvironmentReloadResponse().WorkerMetadata) {
		t.Fatal("init and environment reload inventories differ")
	}
}
