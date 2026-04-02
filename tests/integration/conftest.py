"""Shared fixtures for integration tests."""

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

# Resolve paths
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SAMPLES_DIR = REPO_ROOT / "samples"

# Find func.exe — check FUNC_EXE env var, then common locations
FUNC_EXE = os.environ.get("FUNC_EXE")
if not FUNC_EXE:
    # Try common dev location
    candidate = Path(os.environ.get("FUNC_CLI_PATH", "")) / "func.exe"
    if candidate.exists():
        FUNC_EXE = str(candidate)
    else:
        # Fall back to PATH
        FUNC_EXE = "func"


class FuncHostProcess:
    """Manages a func.exe process for a sample directory."""

    def __init__(self, sample_dir: Path, port: int, env: dict[str, str], log_file: Path):
        self.sample_dir = sample_dir
        self.port = port
        self.env = env
        self.log_file = log_file
        self.process: subprocess.Popen | None = None

    def start(self, build_timeout: int = 60, init_timeout: int = 30):
        """Build the Go binary and start func host."""
        # Build
        result = subprocess.run(
            ["go", "build", "-o", "app.exe", "."],
            cwd=self.sample_dir,
            capture_output=True,
            text=True,
            timeout=build_timeout,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Go build failed in {self.sample_dir}:\n{result.stderr}")

        # Prepare env
        run_env = os.environ.copy()
        run_env.update(self.env)

        # Start func host
        self._log_fh = open(self.log_file, "w")
        self.process = subprocess.Popen(
            [FUNC_EXE, "start", "--port", str(self.port)],
            cwd=self.sample_dir,
            env=run_env,
            stdout=self._log_fh,
            stderr=subprocess.STDOUT,
        )

        # Wait for worker initialization
        self._wait_for_pattern("Worker process started and initialized", timeout=init_timeout)

    def stop(self):
        """Kill the func host process."""
        if self.process and self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=5)
        if hasattr(self, "_log_fh") and self._log_fh:
            self._log_fh.close()

    def read_log(self) -> str:
        """Read the current log file contents."""
        self._log_fh.flush()
        return self.log_file.read_text(errors="replace")

    def wait_for_invocation(self, pattern: str, timeout: int = 30) -> bool:
        """Wait until a pattern appears in the log file."""
        return self._wait_for_pattern(pattern, timeout)

    def assert_log_contains(self, pattern: str, timeout: int = 30):
        """Assert that a pattern appears in the log within timeout."""
        if not self._wait_for_pattern(pattern, timeout):
            log_content = self.read_log()
            last_lines = "\n".join(log_content.splitlines()[-20:])
            raise AssertionError(
                f"Pattern '{pattern}' not found in log within {timeout}s.\n"
                f"Last 20 lines:\n{last_lines}"
            )

    def _wait_for_pattern(self, pattern: str, timeout: int) -> bool:
        """Poll the log file for a pattern."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.process and self.process.poll() is not None:
                # Process exited — check log one more time
                log = self.log_file.read_text(errors="replace")
                return pattern in log
            try:
                log = self.log_file.read_text(errors="replace")
                if pattern in log:
                    return True
            except FileNotFoundError:
                pass
            time.sleep(1)
        return False


@pytest.fixture
def func_host():
    """Fixture factory that starts a func host for a sample directory.

    Usage in a test:
        proc = func_host("httpTrigger", 7201, {"VAR": "value"})
        proc.assert_log_contains("Succeeded")
    """
    processes: list[FuncHostProcess] = []

    def _start(sample_name: str, port: int, env: dict[str, str], init_timeout: int = 30) -> FuncHostProcess:
        sample_dir = SAMPLES_DIR / sample_name
        if not sample_dir.exists():
            raise FileNotFoundError(f"Sample directory not found: {sample_dir}")

        log_file = Path(tempfile.mktemp(suffix=f"_{sample_name}.log"))
        proc = FuncHostProcess(sample_dir, port, env, log_file)
        proc.start(init_timeout=init_timeout)
        processes.append(proc)
        return proc

    yield _start

    # Cleanup all processes
    for proc in processes:
        proc.stop()
