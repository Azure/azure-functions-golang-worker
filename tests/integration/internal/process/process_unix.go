//go:build !windows

package process

import (
	"fmt"
	"os/exec"
	"syscall"
)

func Configure(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func TerminateTree(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return TerminateTreePID(cmd.Process.Pid)
}

// TerminateTreePID kills the process group rooted at pid.
func TerminateTreePID(pid int) error {
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
		return fmt.Errorf("kill process group: %w", err)
	}
	return nil
}
