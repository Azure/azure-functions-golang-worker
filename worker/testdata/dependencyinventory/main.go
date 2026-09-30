package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker"
	"golang.org/x/text/language"
)

func main() {
	// Bound the fixture even if its parent or the proxy exits unexpectedly.
	time.AfterFunc(45*time.Second, func() { os.Exit(124) })
	control, err := net.DialTimeout("tcp", os.Getenv("DEPENDENCY_INVENTORY_CONTROL"), 5*time.Second)
	if err != nil {
		panic(err)
	}
	defer control.Close()
	mutation := os.Getenv("DEPENDENCY_INVENTORY_EXECUTABLE_MUTATION")
	if mutation != "" {
		if runtime.GOOS != "linux" {
			panic("executable mutation requires Linux")
		}
		executable, err := os.Executable()
		if err != nil {
			panic(err)
		}
		// The test runs mutation cases from a disposable copy in t.TempDir.
		// Replace atomically rather than trying to write the executing inode.
		switch mutation {
		case "replace":
			replacement, err := os.CreateTemp(filepath.Dir(executable), "inventory-replacement-*")
			if err != nil {
				panic(err)
			}
			defer os.Remove(replacement.Name())
			_, writeErr := replacement.WriteString("not the running executable\n")
			closeErr := replacement.Close()
			if writeErr != nil {
				panic(writeErr)
			}
			if closeErr != nil {
				panic(closeErr)
			}
			if err := os.Rename(replacement.Name(), executable); err != nil {
				panic(err)
			}
		case "unlink":
			if err := os.Remove(executable); err != nil {
				panic(err)
			}
		default:
			panic("unknown executable mutation: " + mutation)
		}
	}
	if _, err := fmt.Fprintln(control, os.Getpid()); err != nil {
		panic(err)
	}
	if mutation != "" {
		// Acknowledge mutation with the PID, then wait for the test to check
		// the inode and pathname before the first worker metadata read.
		var resume [1]byte
		if _, err := io.ReadFull(control, resume[:]); err != nil {
			if err == io.EOF {
				return // The test failed before allowing startup.
			}
			panic(err)
		}
		if resume[0] != 'S' {
			panic("unexpected fixture startup command")
		}
	}
	go func() {
		// Closing the test-owned connection also stops a child behind a proxy.
		_, _ = io.Copy(io.Discard, control)
		// Close explicitly before Exit so Windows sends FIN instead of resetting
		// the socket during process teardown.
		_ = control.Close()
		os.Exit(0)
	}()

	// Exercise an external package in the compiled app, not just in its tests.
	if language.Make("en-US").String() != "en-US" {
		panic("unexpected language tag")
	}
	worker.Start(sdk.FunctionApp())
}
