package worker

import (
	"bytes"
	"encoding/json"
	"runtime/debug"
	"slices"
)

type dependencyModule struct {
	Path        string                 `json:"path"`
	Version     string                 `json:"version,omitempty"`
	Replacement *dependencyReplacement `json:"replacement,omitempty"`
}

type dependencyReplacement struct {
	Path    string `json:"path,omitempty"`
	Version string `json:"version,omitempty"`
}

type dependencyInventory struct {
	SchemaVersion int               `json:"schema_version"`
	Status        string            `json:"status"`
	Total         int               `json:"total"`
	Reported      int               `json:"reported"`
	Truncated     bool              `json:"truncated"`
	Modules       []json.RawMessage `json:"modules"`
}

func buildDependencyInventory(bi *debug.BuildInfo) string {
	inventory := dependencyInventory{
		SchemaVersion: 1,
		Status:        "unavailable",
		Modules:       []json.RawMessage{},
	}
	if bi != nil {
		inventory.Status = "available"
		inventory.Modules = make([]json.RawMessage, 0, len(bi.Deps))
		for _, dep := range bi.Deps {
			if dep == nil || dep.Path == "" {
				continue
			}
			module := dependencyModule{Path: dep.Path, Version: dep.Version}
			if replacement := dep.Replace; replacement != nil {
				module.Replacement = &dependencyReplacement{}
				if replacement.Version == "" || replacement.Version == "(devel)" {
					// Local replacements have no recorded effective version. Do
					// not send their directory or imply the requested pin ran.
					module.Version = ""
				} else {
					module.Replacement.Path = replacement.Path
					module.Replacement.Version = replacement.Version
				}
			}
			// Only strings and a nonrecursive struct are serialized here, so
			// Marshal cannot encounter unsupported types or reference cycles.
			encoded, _ := json.Marshal(module)
			inventory.Modules = append(inventory.Modules, json.RawMessage(encoded))
		}
	}
	// Sort and deduplicate the normalized records, not the caller's BuildInfo.
	// Distinct versions/replacements for the same path remain separate records.
	slices.SortFunc(inventory.Modules, func(a, b json.RawMessage) int { return bytes.Compare(a, b) })
	inventory.Modules = slices.CompactFunc(inventory.Modules, func(a, b json.RawMessage) bool { return bytes.Equal(a, b) })
	inventory.Total = len(inventory.Modules)
	inventory.Reported = inventory.Total
	// Keep the schema's reported/truncated fields, but do not truncate locally.
	// Host transport and telemetry limits may still reject or truncate an event.
	encoded, _ := json.Marshal(inventory)
	return string(encoded)
}
