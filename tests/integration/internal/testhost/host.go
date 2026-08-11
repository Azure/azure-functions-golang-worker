// Package testhost starts and manages an Azure Functions Core Tools host
// process for integration tests.
package testhost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/azure/azure-functions-golang-worker/tests/integration/internal/process"
)

const cleanupTimeout = 10 * time.Second

// Config holds the parameters needed to launch a test host process.
type Config struct {
	// SampleDir is the directory of the Azure Functions sample to run.
	// It is used as the process working directory and as the basis for the log file name.
	SampleDir string

	// FuncExe is the path to the Azure Functions Core Tools executable (func / func.exe).
	FuncExe string

	// Environment contains additional environment variables merged into the host process environment.
	// Variables are appended after the inherited process environment and before the native-worker variables.
	Environment map[string]string

	// ArtifactDir is the directory where the host log file is written.
	ArtifactDir string

	// InitTimeout limits how long Start waits for the worker to initialize and the port to become ready.
	// Values <= 0 default to 30 seconds.
	InitTimeout time.Duration
}

// Host represents a running Azure Functions Core Tools host process.
type Host interface {
	// URL returns the base HTTP URL ("http://host:port") of the running host.
	URL() string

	// LogPath returns the path of the combined stdout/stderr log file for the host process.
	LogPath() string

	// WaitForLog polls the host log until the given substring appears, the context is cancelled,
	// or the host process exits unexpectedly.
	WaitForLog(context.Context, string) error

	// Stop terminates the host process tree and closes the log file.
	Stop(context.Context) error
}

// host is the internal implementation of Host.
//
// Concurrency contract:
//   - exitDone is closed exactly once by the background goroutine in Start after cmd.Wait returns.
//     Any reader may safely select on it to detect process termination without holding a lock.
//   - exitErr is written once (under exitMu) before exitDone is closed, and is read afterwards via processExitError.
//   - closeLog uses a sync.Once so that both Stop and any cleanup path in Start can call logFile.Close
//     without a double-close.
type host struct {
	address  string
	logPath  string
	logFile  *os.File
	cmd      *exec.Cmd
	exitDone chan struct{}
	exitErr  error
	exitMu   sync.Mutex
	closeLog sync.Once
}

// URL returns the base HTTP URL of the running host.
func (h *host) URL() string {
	return "http://" + h.address
}

// LogPath returns the path of the combined host stdout/stderr log file.
// The log is written continuously while the process runs and supports both
// readiness polling (WaitForLog) and post-failure diagnostics.
func (h *host) LogPath() string {
	return h.logPath
}

// WaitForLog checks existing output once, then reads only new output until the
// expected message appears, the wait ends, or the host stops unexpectedly.
func (h *host) WaitForLog(ctx context.Context, pattern string) error {
	if pattern == "" {
		return nil
	}

	logReader, err := os.Open(h.logPath)
	if err != nil {
		return fmt.Errorf("open host log: %w", err)
	}
	defer logReader.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	buffer := make([]byte, 32*1024)
	overlap := ""
	for {
		for {
			bytesRead, readErr := logReader.Read(buffer)
			if bytesRead > 0 {
				content := overlap + string(buffer[:bytesRead])
				if strings.Contains(content, pattern) {
					return nil
				}
				overlapLength := min(len(pattern)-1, len(content))
				overlap = content[len(content)-overlapLength:]
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return fmt.Errorf("read host log: %w", readErr)
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for host log pattern %q: %w", pattern, ctx.Err())
		case <-h.exitDone:
			content, _ := os.ReadFile(h.logPath)
			return fmt.Errorf("host process exited before log pattern %q: %v\n%s",
				pattern, h.processExitError(), content)
		case <-ticker.C:
		}
	}
}

// waitForPort polls h.address every 100 ms until a TCP connection succeeds,
// the context deadline is exceeded, or the host process exits unexpectedly.
// Port readiness is checked after worker-log readiness so the two checks share
// a single timeout that started in waitUntilReady.
func (h *host) waitForPort(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	dialer := net.Dialer{Timeout: 500 * time.Millisecond}
	for {
		conn, err := dialer.DialContext(ctx, "tcp", h.address)
		if err == nil {
			conn.Close()
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for host port %s: %w", h.address, ctx.Err())
		case <-h.exitDone:
			content, _ := os.ReadFile(h.logPath)
			return fmt.Errorf("host process exited before port %s was ready: %v\n%s",
				h.address, h.processExitError(), content)
		case <-ticker.C:
		}
	}
}

// Stop signals the host process tree to terminate and waits for it to exit,
// then closes the log file. It is safe to call Stop after the process has
// already exited.
func (h *host) Stop(ctx context.Context) error {
	if h.cmd != nil && h.cmd.Process != nil {
		select {
		case <-h.exitDone:
		default:
			// Stop Core Tools and the Go worker together so no test processes
			// are left running.
			if err := process.TerminateTree(h.cmd); err != nil {
				return err
			}
		}
	}

	select {
	case <-h.exitDone:
	case <-ctx.Done():
		return fmt.Errorf("stop host process: %w", ctx.Err())
	}

	// closeLog guards logFile.Close so it is called at most once across Stop
	// and any early-return cleanup paths in Start.
	var closeErr error
	h.closeLog.Do(func() {
		if h.logFile != nil {
			closeErr = h.logFile.Close()
		}
	})
	return closeErr
}

// processExitError returns the error from cmd.Wait, guarded by exitMu.
// It is safe to call only after exitDone is closed.
func (h *host) processExitError() error {
	h.exitMu.Lock()
	defer h.exitMu.Unlock()
	return h.exitErr
}

// newHostCommand builds the exec.Cmd that runs Core Tools for the given port and log file.
//
// Environment layering (in order):
//  1. Inherited process environment (os.Environ)
//  2. config.Environment overrides
//  3. Native Go worker variables (appended last so they cannot be overridden)
//
// Process handling ensures that stopping a test also stops Core Tools and the
// Go worker processes it started.
func newHostCommand(ctx context.Context, config Config, port string, logFile *os.File) *exec.Cmd {
	cmd := exec.CommandContext(ctx, config.FuncExe, "start", "--port", port)
	cmd.Dir = config.SampleDir
	cmd.Env = os.Environ()
	for key, value := range config.Environment {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	// Native worker variables must come last so they cannot be shadowed by config.Environment.
	cmd.Env = append(cmd.Env,
		"FUNCTIONS_WORKER_RUNTIME=native",
		"FUNCTIONS_CLI_NATIVE_LANGUAGE=go",
	)
	// Both stdout and stderr flow into a single log file for combined readiness polling and diagnostics.
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	process.Configure(cmd)
	// Stop all test host processes when the test is cancelled.
	cmd.Cancel = func() error {
		return process.TerminateTree(cmd)
	}
	return cmd
}

// waitUntilReady checks that the Go worker has initialized and that Core Tools
// is accepting connections, using a single shared timeout for both checks.
// timeout <= 0 defaults to 30 seconds.
func (h *host) waitUntilReady(ctx context.Context, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	initCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Worker-log readiness: confirms that the Go worker process has started and
	// registered itself with Core Tools before we attempt a port check.
	if err := h.WaitForLog(initCtx, "Worker process started and initialized"); err != nil {
		return err
	}
	// Port readiness: confirms that Core Tools is accepting HTTP connections.
	// Uses the same initCtx so both checks share the original timeout budget.
	return h.waitForPort(initCtx)
}

// Start launches the sample application in a local Azure Functions host and
// waits until it is ready to receive requests. It captures startup and runtime
// logs for troubleshooting and cleans up the host if startup fails.
func Start(ctx context.Context, config Config) (Host, error) {
	// Step 1: reserve a free loopback port.
	address, err := reserveLoopbackPort()
	if err != nil {
		return nil, err
	}

	// Step 2: create artifacts directory and host log file.
	if err := os.MkdirAll(config.ArtifactDir, 0o755); err != nil {
		return nil, fmt.Errorf("create artifact directory: %w", err)
	}
	sampleName := filepath.Base(filepath.Clean(config.SampleDir))
	logPath := filepath.Join(config.ArtifactDir, sampleName+"-host.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, fmt.Errorf("create host log: %w", err)
	}

	_, port, err := net.SplitHostPort(address)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("parse host address: %w", err)
	}

	// Step 3: build and start the process.
	cmd := newHostCommand(ctx, config, port, logFile)
	started := &host{
		address:  address,
		logPath:  logPath,
		logFile:  logFile,
		cmd:      cmd,
		exitDone: make(chan struct{}),
	}
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, fmt.Errorf("start host on port %s: %w", strconv.Quote(port), err)
	}
	// Background goroutine: records cmd.Wait result and closes exitDone so
	// WaitForLog, waitForPort, and Stop can detect process termination via a select.
	go func() {
		err := cmd.Wait()
		started.exitMu.Lock()
		started.exitErr = err
		started.exitMu.Unlock()
		close(started.exitDone)
	}()

	// Step 4: wait for readiness; clean up on failure.
	if err := started.waitUntilReady(ctx, config.InitTimeout); err != nil {
		// Give cleanup a short, independent window if startup times out.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		stopErr := started.Stop(cleanupCtx)
		cancel()
		if stopErr != nil {
			return nil, errors.Join(err, fmt.Errorf("clean up failed host startup: %w", stopErr))
		}
		return nil, err
	}
	return started, nil
}

// reserveLoopbackPort opens a listener on 127.0.0.1:0 to obtain a free port
// assigned by the OS, then immediately closes it. The caller is expected to
// pass the returned address to Core Tools, which will bind to the same port.
// The listener is released before Core Tools starts to avoid an "address already
// in use" error; there is a small TOCTOU window that is acceptable in local test environments.
func reserveLoopbackPort() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("reserve loopback port: %w", err)
	}

	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return "", fmt.Errorf("release loopback port: %w", err)
	}
	return address, nil
}
