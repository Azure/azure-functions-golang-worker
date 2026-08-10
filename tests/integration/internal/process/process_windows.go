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
	kill := exec.Command("taskkill", "/F", "/T", "/PID", strconv.Itoa(cmd.Process.Pid))
	if output, err := kill.CombinedOutput(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 128 {
			return nil
		}
		return fmt.Errorf("terminate process tree: %w: %s", err, output)
	}
	return nil
}
