package testhost

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	SampleDir   string
	FuncExe     string
	Environment map[string]string
	ArtifactDir string
	InitTimeout time.Duration
}

type Host interface {
	URL() string
	LogPath() string
	WaitForLog(context.Context, string) error
	Stop(context.Context) error
}

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

func (h *host) URL() string {
	return "http://" + h.address
}

func (h *host) LogPath() string {
	return h.logPath
}

func (h *host) WaitForLog(ctx context.Context, pattern string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		content, err := os.ReadFile(h.logPath)
		if err != nil {
			return fmt.Errorf("read host log: %w", err)
		}
		if strings.Contains(string(content), pattern) {
			return nil
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

func (h *host) Stop(ctx context.Context) error {
	if h.cmd != nil && h.cmd.Process != nil {
		select {
		case <-h.exitDone:
		default:
			if err := terminateProcessTree(h.cmd); err != nil {
				return err
			}
		}
	}

	select {
	case <-h.exitDone:
	case <-ctx.Done():
		return fmt.Errorf("stop host process: %w", ctx.Err())
	}

	var closeErr error
	h.closeLog.Do(func() {
		if h.logFile != nil {
			closeErr = h.logFile.Close()
		}
	})
	return closeErr
}

func (h *host) processExitError() error {
	h.exitMu.Lock()
	defer h.exitMu.Unlock()
	return h.exitErr
}

func Start(ctx context.Context, config Config) (Host, error) {
	address, err := reserveLoopbackPort()
	if err != nil {
		return nil, err
	}
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

	cmd := exec.CommandContext(ctx, config.FuncExe, "start", "--port", port)
	cmd.Dir = config.SampleDir
	cmd.Env = os.Environ()
	for key, value := range config.Environment {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	cmd.Env = append(cmd.Env,
		"FUNCTIONS_WORKER_RUNTIME=native",
		"FUNCTIONS_CLI_NATIVE_LANGUAGE=go",
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	configureProcess(cmd)
	cmd.Cancel = func() error {
		return terminateProcessTree(cmd)
	}

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
	go func() {
		err := cmd.Wait()
		started.exitMu.Lock()
		started.exitErr = err
		started.exitMu.Unlock()
		close(started.exitDone)
	}()

	initTimeout := config.InitTimeout
	if initTimeout <= 0 {
		initTimeout = 30 * time.Second
	}
	initCtx, cancel := context.WithTimeout(ctx, initTimeout)
	defer cancel()
	if err := started.WaitForLog(initCtx, "Worker process started and initialized"); err != nil {
		_ = started.Stop(context.Background())
		return nil, err
	}
	if err := started.waitForPort(initCtx); err != nil {
		_ = started.Stop(context.Background())
		return nil, err
	}
	return started, nil
}

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
