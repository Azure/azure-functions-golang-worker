//go:build windows

package testhost

import (
	"errors"
	"fmt"
	"os/exec"
	"strconv"
)

func configureProcess(*exec.Cmd) {}

func terminateProcessTree(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	kill := exec.Command("taskkill", "/F", "/T", "/PID", strconv.Itoa(cmd.Process.Pid))
	if output, err := kill.CombinedOutput(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 128 {
			return nil
		}
		return fmt.Errorf("terminate host process tree: %w: %s", err, output)
	}
	return nil
}
