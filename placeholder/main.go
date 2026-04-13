// Placeholder is a minimal Go binary that satisfies the Azure Functions host
// during placeholder mode. It connects to the host's gRPC endpoint, responds
// to initialization, and reports zero functions.
//
// It watches /home/site/wwwroot/ via inotify for the real app binary to be
// deployed. When it detects a CREATE event for "app", it exits so the host
// restarts and launches the real binary.
package main

import (
	"log"
	"syscall"
	"unsafe"

	"github.com/azure/azure-functions-golang-worker/sdk"
	"github.com/azure/azure-functions-golang-worker/worker"
)

const watchDir = "/home/site/wwwroot"

func main() {
	log.Println("Placeholder: starting...")
	go watchForDeployment()
	app := sdk.FunctionApp()
	worker.Start(app)
}

// watchForDeployment uses inotify to detect when the real app binary is deployed.
// When a CREATE event for "app" is detected, the placeholder exits.
func watchForDeployment() {
	fd, err := syscall.InotifyInit()
	if err != nil {
		log.Printf("Placeholder: inotify init failed: %v", err)
		return
	}

	_, err = syscall.InotifyAddWatch(fd, watchDir, syscall.IN_CLOSE_WRITE)
	if err != nil {
		log.Printf("Placeholder: inotify watch failed on %s: %v", watchDir, err)
		syscall.Close(fd)
		return
	}

	log.Printf("Placeholder: watching %s for deployment...", watchDir)

	buf := make([]byte, 4096)
	for {
		n, err := syscall.Read(fd, buf)
		if err != nil || n == 0 {
			log.Printf("Placeholder: inotify read error: %v", err)
			return
		}

		offset := 0
		for offset < n {
			event := (*syscall.InotifyEvent)(unsafe.Pointer(&buf[offset]))
			nameBytes := int(event.Len)
			if nameBytes > 0 {
				// Extract the filename, trimming null bytes
				raw := buf[offset+syscall.SizeofInotifyEvent : offset+syscall.SizeofInotifyEvent+nameBytes]
				name := string(raw)
				for i, b := range raw {
					if b == 0 {
						name = string(raw[:i])
						break
					}
				}
				if name == "app" {
					log.Printf("Placeholder: detected new app binary, exiting")
					syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
				}
			}
			offset += syscall.SizeofInotifyEvent + nameBytes
		}
	}
}
