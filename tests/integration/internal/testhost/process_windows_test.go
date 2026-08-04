//go:build windows

package testhost

import (
	"os/exec"
	"testing"
)

func TestTerminateProcessTreeAllowsAlreadyExitedProcess(t *testing.T) {
	cmd := exec.Command("powershell", "-NoProfile", "-Command", "exit 0")
	if err := cmd.Run(); err != nil {
		t.Fatalf("run completed process: %v", err)
	}

	if err := terminateProcessTree(cmd); err != nil {
		t.Fatalf("terminateProcessTree() error = %v, want nil for exited process", err)
	}
}
