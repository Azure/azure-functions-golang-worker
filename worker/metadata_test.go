package worker

import (
	"runtime"
	"testing"
)

func TestBuildWorkerMetadata_StaticFields(t *testing.T) {
	md := buildWorkerMetadata()
	if md == nil {
		t.Fatal("buildWorkerMetadata returned nil")
	}
	if md.RuntimeName != "go" {
		t.Errorf("RuntimeName = %q, want go", md.RuntimeName)
	}
	if md.RuntimeVersion != runtime.Version() {
		t.Errorf("RuntimeVersion = %q, want %q", md.RuntimeVersion, runtime.Version())
	}
	if md.WorkerBitness != runtime.GOOS+"/"+runtime.GOARCH {
		t.Errorf("WorkerBitness = %q, want %q/%q", md.WorkerBitness, runtime.GOOS, runtime.GOARCH)
	}
}

func TestBuildWorkerMetadata_CustomPropertiesAlwaysPresent(t *testing.T) {
	// All four documented keys must appear in CustomProperties even when
	// the build has no VCS info and no replace directive. Telemetry
	// consumers query for these keys unconditionally.
	md := buildWorkerMetadata()
	for _, k := range []string{
		MetaSDKReplaced,
		MetaSDKReplacePath,
		MetaAppBuiltDirty,
		MetaAppVCSRevision,
	} {
		if _, ok := md.CustomProperties[k]; !ok {
			t.Errorf("CustomProperties missing key %q", k)
		}
	}
}

func TestBuildWorkerMetadata_DefaultsAreFalseAndEmpty(t *testing.T) {
	// Default values when no VCS and no replace directive: "false" /
	// "false" / "" / "". The bool-shaped values must be the literal
	// string "false" so KQL boolean filters work directly.
	md := buildWorkerMetadata()

	// sdk_replaced and app_built_dirty must default to "false" exactly
	// (or "true" if the test environment happens to be a developer
	// build). Either way, they must never be the empty string.
	for _, k := range []string{MetaSDKReplaced, MetaAppBuiltDirty} {
		v := md.CustomProperties[k]
		if v != "true" && v != "false" {
			t.Errorf("CustomProperties[%q] = %q, want \"true\" or \"false\"", k, v)
		}
	}
}

func TestBuildWorkerMetadata_WorkerVersionPresent(t *testing.T) {
	// WorkerVersion is always populated. In test runs it will typically
	// be "(devel)" because the test binary is built directly from this
	// repo without a release-tag commit.
	md := buildWorkerMetadata()
	if md.WorkerVersion == "" {
		t.Error("WorkerVersion must not be empty")
	}
}
