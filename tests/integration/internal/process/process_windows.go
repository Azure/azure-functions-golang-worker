//go:build windows

package process

import (
	"errors"
	"fmt"
	"os/exec"
	"strconv"
)

func Configure(*exec.Cmd) {}

func TerminateTree(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return TerminateTreePID(cmd.Process.Pid)
}

// TerminateTreePID kills the process tree rooted at pid.
func TerminateTreePID(pid int) error {
	kill := exec.Command("taskkill", "/F", "/T", "/PID", strconv.Itoa(pid))
	if output, err := kill.CombinedOutput(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 128 {
			return nil
		}
		return fmt.Errorf("terminate process tree: %w: %s", err, output)
	}
	return nil
}
