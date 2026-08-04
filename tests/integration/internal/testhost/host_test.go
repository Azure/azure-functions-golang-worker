package testhost

import (
	"context"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestReserveLoopbackPortReturnsAvailableAddress(t *testing.T) {
	address, err := reserveLoopbackPort()
	if err != nil {
		t.Fatalf("reserveLoopbackPort() error = %v", err)
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("reserved address %q is not available: %v", address, err)
	}
	listener.Close()
}

func TestHostContract(t *testing.T) {
	var _ Host = (*host)(nil)

	config := Config{
		SampleDir:   "sample",
		FuncExe:     "func",
		Environment: map[string]string{"FUNCTIONS_WORKER_RUNTIME": "native"},
		ArtifactDir: "artifacts",
		InitTimeout: 30 * time.Second,
	}

	if config.SampleDir == "" || config.FuncExe == "" || config.ArtifactDir == "" {
		t.Fatal("Config fields should preserve their values")
	}
}

func TestStartCapturesHostOutputInArtifactDirectory(t *testing.T) {
	funcExe := buildFakeFunc(t, `
package main

import (
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	port := os.Args[len(os.Args)-1]
	listener, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	fmt.Println("Worker process started and initialized")
	fmt.Println("runtime=" + os.Getenv("FUNCTIONS_WORKER_RUNTIME"))
	fmt.Println("language=" + os.Getenv("FUNCTIONS_CLI_NATIVE_LANGUAGE"))
	fmt.Println("fake host output")
	for {
		time.Sleep(time.Second)
	}
}
`)
	artifactDir := t.TempDir()

	started, err := Start(context.Background(), Config{
		SampleDir:   t.TempDir(),
		FuncExe:     funcExe,
		ArtifactDir: artifactDir,
		Environment: map[string]string{
			"FUNCTIONS_WORKER_RUNTIME": "golang",
		},
		InitTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		if err := started.Stop(context.Background()); err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})

	hostURL, err := url.Parse(started.URL())
	if err != nil {
		t.Fatalf("URL() returned invalid URL %q: %v", started.URL(), err)
	}
	if hostURL.Hostname() != "127.0.0.1" || hostURL.Port() == "" {
		t.Fatalf("URL() = %q, want dynamic loopback address", started.URL())
	}

	logPath, err := filepath.Abs(started.LogPath())
	if err != nil {
		t.Fatalf("resolve LogPath(): %v", err)
	}
	artifactPath, err := filepath.Abs(artifactDir)
	if err != nil {
		t.Fatalf("resolve artifact directory: %v", err)
	}
	if !strings.HasPrefix(logPath, artifactPath+string(os.PathSeparator)) {
		t.Fatalf("LogPath() = %q, want path beneath %q", logPath, artifactPath)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := started.WaitForLog(ctx, "fake host output"); err != nil {
		t.Fatalf("WaitForLog() error = %v", err)
	}

	logContent, err := os.ReadFile(started.LogPath())
	if err != nil {
		t.Fatalf("read host log: %v", err)
	}
	if !strings.Contains(string(logContent), "fake host output") {
		t.Fatalf("host log does not contain fake process output:\n%s", logContent)
	}
	if !strings.Contains(string(logContent), "runtime=native") ||
		!strings.Contains(string(logContent), "language=go") {
		t.Fatalf("host log does not contain native Go worker environment:\n%s", logContent)
	}
}

func TestStartWaitsForHostPortAfterWorkerInitialization(t *testing.T) {
	funcExe := buildFakeFunc(t, `
package main

import (
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	fmt.Println("Worker process started and initialized")
	time.Sleep(750 * time.Millisecond)
	port := os.Args[len(os.Args)-1]
	listener, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	for {
		time.Sleep(time.Second)
	}
}
`)

	startedAt := time.Now()
	started, err := Start(context.Background(), Config{
		SampleDir:   t.TempDir(),
		FuncExe:     funcExe,
		ArtifactDir: t.TempDir(),
		InitTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = started.Stop(context.Background())
	})

	if elapsed := time.Since(startedAt); elapsed < 700*time.Millisecond {
		t.Fatalf("Start() returned after %v, before host port was ready", elapsed)
	}

	address := strings.TrimPrefix(started.URL(), "http://")
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatalf("host port is not ready after Start(): %v", err)
	}
	conn.Close()
}

func TestStartReportsProcessExitBeforeInitialization(t *testing.T) {
	funcExe := buildFakeFunc(t, `
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("fatal fake host error")
	os.Exit(23)
}
`)

	startedAt := time.Now()
	_, err := Start(context.Background(), Config{
		SampleDir:   t.TempDir(),
		FuncExe:     funcExe,
		ArtifactDir: t.TempDir(),
		InitTimeout: 5 * time.Second,
	})
	if err == nil {
		t.Fatal("Start() error = nil, want process exit error")
	}
	if elapsed := time.Since(startedAt); elapsed >= 4*time.Second {
		t.Fatalf("Start() reported process exit after %v, want immediate failure", elapsed)
	}
	if !strings.Contains(err.Error(), "exited") || !strings.Contains(err.Error(), "fatal fake host error") {
		t.Fatalf("Start() error = %q, want exit status and host log", err)
	}
}

func TestStopTerminatesChildProcesses(t *testing.T) {
	childAddress, err := reserveLoopbackPort()
	if err != nil {
		t.Fatalf("reserve child port: %v", err)
	}
	pidPath := filepath.Join(t.TempDir(), "child.pid")
	funcExe := buildFakeFunc(t, `
package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "child" {
		listener, err := net.Listen("tcp", os.Getenv("CHILD_ADDRESS"))
		if err != nil {
			panic(err)
		}
		defer listener.Close()
		pid := strconv.Itoa(os.Getpid())
		if err := os.WriteFile(os.Getenv("CHILD_PID_PATH"), []byte(pid), 0600); err != nil {
			panic(err)
		}
		for {
			time.Sleep(time.Second)
		}
	}

	child := exec.Command(os.Args[0], "child")
	child.Env = os.Environ()
	if err := child.Start(); err != nil {
		panic(err)
	}
	port := os.Args[len(os.Args)-1]
	hostListener, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		panic(err)
	}
	defer hostListener.Close()
	for i := 0; i < 50; i++ {
		conn, err := net.DialTimeout("tcp", os.Getenv("CHILD_ADDRESS"), 100*time.Millisecond)
		if err == nil {
			conn.Close()
			fmt.Println("Worker process started and initialized")
			for {
				time.Sleep(time.Second)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	panic("child did not listen")
}
`)

	started, err := Start(context.Background(), Config{
		SampleDir:   t.TempDir(),
		FuncExe:     funcExe,
		ArtifactDir: t.TempDir(),
		Environment: map[string]string{
			"CHILD_ADDRESS":  childAddress,
			"CHILD_PID_PATH": pidPath,
		},
		InitTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		pidBytes, readErr := os.ReadFile(pidPath)
		if readErr != nil {
			return
		}
		pid, parseErr := strconv.Atoi(string(pidBytes))
		if parseErr != nil {
			return
		}
		if process, findErr := os.FindProcess(pid); findErr == nil {
			_ = process.Kill()
		}
	})

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := started.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	listener, err := net.Listen("tcp", childAddress)
	if err != nil {
		t.Fatalf("child process still owns %s after Stop(): %v", childAddress, err)
	}
	listener.Close()

	if err := started.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop() error = %v, want idempotent cleanup", err)
	}
}

func TestCancelingContextTerminatesChildProcesses(t *testing.T) {
	childAddress, err := reserveLoopbackPort()
	if err != nil {
		t.Fatalf("reserve child port: %v", err)
	}
	funcExe := buildFakeFunc(t, `
package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"time"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "child" {
		listener, err := net.Listen("tcp", os.Getenv("CHILD_ADDRESS"))
		if err != nil {
			panic(err)
		}
		defer listener.Close()
		for {
			time.Sleep(time.Second)
		}
	}

	child := exec.Command(os.Args[0], "child")
	child.Env = os.Environ()
	if err := child.Start(); err != nil {
		panic(err)
	}

	port := os.Args[len(os.Args)-1]
	hostListener, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		panic(err)
	}
	defer hostListener.Close()

	for i := 0; i < 50; i++ {
		conn, err := net.DialTimeout("tcp", os.Getenv("CHILD_ADDRESS"), 100*time.Millisecond)
		if err == nil {
			conn.Close()
			fmt.Println("Worker process started and initialized")
			for {
				time.Sleep(time.Second)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	panic("child did not listen")
}
`)

	hostCtx, cancelHost := context.WithCancel(context.Background())
	started, err := Start(hostCtx, Config{
		SampleDir:   t.TempDir(),
		FuncExe:     funcExe,
		ArtifactDir: t.TempDir(),
		Environment: map[string]string{
			"CHILD_ADDRESS": childAddress,
		},
		InitTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	cancelHost()

	deadline := time.Now().Add(5 * time.Second)
	for {
		listener, listenErr := net.Listen("tcp", childAddress)
		if listenErr == nil {
			listener.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("child process still owns %s after context cancellation: %v", childAddress, listenErr)
		}
		time.Sleep(100 * time.Millisecond)
	}

	if err := started.Stop(context.Background()); err != nil {
		t.Fatalf("Stop() after context cancellation error = %v", err)
	}
}

func buildFakeFunc(t *testing.T, source string) string {
	t.Helper()

	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "main.go")
	if err := os.WriteFile(sourcePath, []byte(source), 0o600); err != nil {
		t.Fatalf("write fake func source: %v", err)
	}

	exeName := "fakefunc"
	if runtime.GOOS == "windows" {
		exeName += ".exe"
	}
	exePath := filepath.Join(dir, exeName)
	cmd := exec.Command("go", "build", "-o", exePath, sourcePath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fake func: %v\n%s", err, output)
	}
	return exePath
}
