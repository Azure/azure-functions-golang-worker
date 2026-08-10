//go:build windows

package process

import (
	"os/exec"
	"testing"
)

func TestTerminateTreeAllowsAlreadyExitedProcess(t *testing.T) {
	cmd := exec.Command("powershell", "-NoProfile", "-Command", "exit 0")
	if err := cmd.Run(); err != nil {
		t.Fatalf("run completed process: %v", err)
	}

	if err := TerminateTree(cmd); err != nil {
		t.Fatalf("TerminateTree() error = %v, want nil for exited process", err)
	}
}
