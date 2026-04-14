package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/azure/azure-functions-golang-worker/worker"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultAppDir = "/home/site/wwwroot"
const defaultAppName = "app"

// appBinaryPath returns the full path to the user's app binary,
// using FUNCTIONS_APP_BINARY_NAME env var with a default of "app".
func appBinaryPath(dir string) string {
	name := os.Getenv("FUNCTIONS_APP_BINARY_NAME")
	if name == "" {
		name = defaultAppName
	}
	return filepath.Join(dir, name)
}

type Proxy struct {
	pb.UnimplementedFunctionRpcServer

	hostClient pb.FunctionRpcClient
	hostStream grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage]

	childStream    pb.FunctionRpc_EventStreamServer
	childConnected chan struct{}
	childInitDone  chan struct{}

	config   *worker.WorkerStartupConfig
	server   *grpc.Server
	listener net.Listener

	savedInitRequest     *pb.StreamingMessage
	savedReloadRequestId string
	childCapabilities    map[string]string
	childWorkerMetadata  *pb.WorkerMetadata
	mutex                sync.Mutex
	hostSendMu           sync.Mutex
	childConnectedOnce   sync.Once
	isSpecializing       bool
}

// sendToHost sends a message to the host with mutex protection.
// gRPC stream Send is not safe for concurrent use.
func (p *Proxy) sendToHost(msg *pb.StreamingMessage) error {
	p.hostSendMu.Lock()
	defer p.hostSendMu.Unlock()
	return p.hostStream.Send(msg)
}

func main() {
	log.Println("Go Worker Proxy Starting...")

	// If the real app exists, exec into it directly. This handles the post-
	// specialization case: after deployment places the real binary at
	// /home/site/wwwroot/app, the host restarts and starts the "proxy" again
	// (since defaultExecutablePath points here). Instead of running as a proxy,
	// we replace ourselves with the real app. Zero overhead.
	appPath := appBinaryPath(defaultAppDir)
	if _, err := os.Stat(appPath); err == nil {
		log.Printf("Real app found at %s, exec into it", appPath)
		args := append([]string{appPath}, os.Args[1:]...)
		err := syscall.Exec(appPath, args, os.Environ())
		log.Fatalf("exec failed: %v", err)
	}

	// The proxy only serves a purpose in flex consumption placeholder mode.
	// If the app binary exists, the exec bypass above handles it.
	// If we reach here without placeholder mode, it's a misconfiguration.
	if os.Getenv("WEBSITE_PLACEHOLDER_MODE") != "1" {
		log.Fatalf("Proxy requires WEBSITE_PLACEHOLDER_MODE=1. "+
			"App binary not found at %s and not in placeholder mode.", appPath)
	}

	// 1. Parse Args (same as Worker)
	config, err := worker.GetWorkerStartupConfig()
	if err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	p := &Proxy{
		config:         config,
		childConnected: make(chan struct{}),
		childInitDone:  make(chan struct{}),
	}

	// 2. Connect to Host
	log.Printf("Connecting to Host at %s", config.FunctionsUri)
	if err := p.connectToHost(); err != nil {
		log.Fatalf("Failed to connect to host: %v", err)
	}

	// 3. Start Listening for Child (on random port)
	if err := p.startLocalServer(); err != nil {
		log.Fatalf("Failed to start local server: %v", err)
	}
	defer p.server.Stop()

	// 4. Start Main Loop
	p.run()
}

func (p *Proxy) connectToHost() error {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(p.config.FunctionsGrpcMaxMessageLength),
			grpc.MaxCallSendMsgSize(p.config.FunctionsGrpcMaxMessageLength),
		),
	}

	conn, err := grpc.NewClient(p.config.FunctionsUri, opts...)
	if err != nil {
		return err
	}

	p.hostClient = pb.NewFunctionRpcClient(conn)
	stream, err := p.hostClient.EventStream(context.Background())
	if err != nil {
		return err
	}
	p.hostStream = stream

	// Send StartStream to Host
	log.Printf("Sending StartStream to Host (WorkerID: %s)", p.config.FunctionsWorkerId)
	return p.sendToHost(&pb.StreamingMessage{
		RequestId: p.config.FunctionsRequestId,
		Content: &pb.StreamingMessage_StartStream{
			StartStream: &pb.StartStream{
				WorkerId: p.config.FunctionsWorkerId,
			},
		},
	})
}

func (p *Proxy) startLocalServer() error {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	p.listener = lis
	log.Printf("Proxy listening on %s", lis.Addr().String())

	p.server = grpc.NewServer()
	pb.RegisterFunctionRpcServer(p.server, p)

	go func() {
		if err := p.server.Serve(lis); err != nil {
			log.Printf("Local gRPC server stopped: %v", err)
		}
	}()
	return nil
}

// Implement FunctionRpcServer.EventStream (Accepts connection from Child)
func (p *Proxy) EventStream(stream pb.FunctionRpc_EventStreamServer) error {
	log.Println("Child connected to Proxy")
	p.mutex.Lock()
	p.childStream = stream
	p.mutex.Unlock()

	p.childConnectedOnce.Do(func() { close(p.childConnected) })

	// Wait until stream closes
	<-stream.Context().Done()
	return nil
}

func (p *Proxy) run() {
	// Loop reading from Host
	for {
		msg, err := p.hostStream.Recv()
		if err == io.EOF {
			log.Println("Host closed stream")
			return
		}
		if err != nil {
			log.Fatalf("Error receiving from host: %v", err)
		}

		p.handleHostMessage(msg)
	}
}

func (p *Proxy) handleHostMessage(msg *pb.StreamingMessage) {
	// Check if we are specialized/Proxying
	p.mutex.Lock()
	childReady := p.childStream != nil
	specializing := p.isSpecializing
	p.mutex.Unlock()

	if childReady {
		if err := p.childStream.Send(msg); err != nil {
			log.Printf("Error sending to child: %v", err)
		}
		return
	}

	// If we're specializing (env reload in progress),
	// wait for the child to be fully initialized before forwarding.
	// This ensures the child has received its WorkerInitRequest and responded
	// before any host messages (like FunctionsMetadataRequest) reach it.
	if specializing {
		log.Printf("Specializing - waiting for child init before forwarding: %T", msg.Content)
		<-p.childInitDone
		if err := p.childStream.Send(msg); err != nil {
			log.Printf("Error forwarding to child during specialization: %v", err)
		}
		return
	}

	// Placeholder Mode Logic
	switch content := msg.Content.(type) {

	case *pb.StreamingMessage_WorkerInitRequest:
		log.Println("Received WorkerInitRequest from Host")
		p.savedInitRequest = msg

		// Respond to Host with placeholder capabilities.
		// These are replaced by the child's real capabilities via the
		// FunctionEnvironmentReloadResponse after specialization.
		response := &pb.StreamingMessage{
			RequestId: msg.RequestId,
			Content: &pb.StreamingMessage_WorkerInitResponse{
				WorkerInitResponse: &pb.WorkerInitResponse{
					Result: &pb.StatusResult{Status: pb.StatusResult_Success},
					Capabilities: map[string]string{
						"TypedDataCollection":               "true",
						"WorkerStatus":                      "true",
						"RpcHttpBodyOnly":                   "true",
						"RawHttpBodyBytes":                  "true",
						"RpcHttpTriggerMetadataRemoved":     "true",
						"UseNullableValueDictionaryForHttp": "true",
						"HandlesWorkerTerminateMessage":     "true",
					},
					WorkerVersion: "1.0.0",
				},
			},
		}
		p.sendToHost(response)

	case *pb.StreamingMessage_WorkerHeartbeat:
		p.sendToHost(&pb.StreamingMessage{
			RequestId: msg.RequestId,
			Content: &pb.StreamingMessage_WorkerHeartbeat{
				WorkerHeartbeat: &pb.WorkerHeartbeat{},
			},
		})

	case *pb.StreamingMessage_FunctionEnvironmentReloadRequest:
		log.Println("Received FunctionEnvironmentReloadRequest - SPECIALIZING")
		// Set isSpecializing before launching goroutine to ensure subsequent
		// messages (like FunctionsMetadataRequest) wait for the child.
		p.mutex.Lock()
		p.isSpecializing = true
		p.savedReloadRequestId = msg.RequestId
		p.mutex.Unlock()
		go p.specialize(content.FunctionEnvironmentReloadRequest)

	case *pb.StreamingMessage_FunctionsMetadataRequest:
		log.Println("Received FunctionsMetadataRequest in placeholder mode - returning empty")
		p.sendToHost(&pb.StreamingMessage{
			RequestId: msg.RequestId,
			Content: &pb.StreamingMessage_FunctionMetadataResponse{
				FunctionMetadataResponse: &pb.FunctionMetadataResponse{
					FunctionMetadataResults: []*pb.RpcFunctionMetadata{},
					Result: &pb.StatusResult{
						Status: pb.StatusResult_Success,
					},
				},
			},
		})

	case *pb.StreamingMessage_WorkerStatusRequest:
		p.sendToHost(&pb.StreamingMessage{
			RequestId: msg.RequestId,
			Content: &pb.StreamingMessage_WorkerStatusResponse{
				WorkerStatusResponse: &pb.WorkerStatusResponse{},
			},
		})

	case *pb.StreamingMessage_WorkerTerminate:
		log.Println("Received WorkerTerminate in placeholder mode, exiting")
		os.Exit(0)

	default:
		log.Printf("Unhandled message in placeholder mode: %T", msg.Content)
	}
}

func (p *Proxy) specialize(req *pb.FunctionEnvironmentReloadRequest) {
	// 1. Apply FERR environment to the proxy process.
	// This keeps the proxy aligned with the child's env and ensures
	// helpers like appBinaryPath() see FERR values (e.g., FUNCTIONS_APP_BINARY_NAME).
	// The child inherits the updated env via cmd.Env = os.Environ().
	for k, v := range req.EnvironmentVariables {
		os.Setenv(k, v)
	}
	dir := req.FunctionAppDirectory

	// 2. Determine App Path
	workerPath := appBinaryPath(dir)

	// 3. Construct Args
	// Point the child to the proxy's local gRPC server, not the host.
	// We reuse the host-assigned worker ID and request ID since the child
	// connects to the proxy (not the host) and its StartStream is dropped.
	_, port, _ := net.SplitHostPort(p.listener.Addr().String())
	proxyUri := fmt.Sprintf("http://127.0.0.1:%s", port)

	args := []string{
		"--functions-uri", proxyUri,
		"--functions-worker-id", p.config.FunctionsWorkerId,
		"--functions-request-id", p.config.FunctionsRequestId,
		"--functions-grpc-max-message-length", fmt.Sprintf("%d", p.config.FunctionsGrpcMaxMessageLength),
	}

	// 4. Start Process
	cmd := exec.Command(workerPath, args...)
	cmd.Env = os.Environ()
	cmd.Dir = dir
	if cmd.Dir == "" {
		cmd.Dir, _ = os.Getwd()
	}

	// Forward stdout/stderr for debugging
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	log.Printf("Starting Child Worker: %s %v in %s", workerPath, args, cmd.Dir)
	if err := cmd.Start(); err != nil {
		log.Fatalf("Failed to start child worker: %v", err)
	}

	// Monitor child process: if the child dies, exit the proxy with the
	// same exit code so the host detects it immediately via Process.Exited
	// and restarts us (at which point the exec bypass kicks in).
	go func() {
		err := cmd.Wait()
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				log.Printf("Child process exited with code %d, propagating", exitErr.ExitCode())
				os.Exit(exitErr.ExitCode())
			}
			log.Printf("Child process failed: %v, exiting", err)
			os.Exit(1)
		}
		log.Println("Child process exited cleanly, exiting proxy")
		os.Exit(0)
	}()

	// 5. Wait for Connection
	log.Println("Waiting for child to connect...")
	<-p.childConnected
	log.Println("Child Connected! Starting Bridge...")

	// 6. Bridge: Handshake
	// Replay the saved WorkerInitRequest to the Child so it can initialize
	log.Println("Replaying WorkerInitRequest to Child")
	p.childStream.Send(p.savedInitRequest)

	// 7. Bridge: Response Loop (Child -> Host)
	go func() {
		for {
			msg, err := p.childStream.Recv()
			if err != nil {
				log.Printf("Error receiving from child: %v", err)
				return
			}

			// Always drop StartStream from child (proxy already sent its own)
			switch msg.Content.(type) {
			case *pb.StreamingMessage_StartStream:
				log.Println("Dropping StartStream from Child")
				continue
			case *pb.StreamingMessage_WorkerInitResponse:
				initResp := msg.GetWorkerInitResponse()
				// Capture child's capabilities for the FERR response
				log.Printf("Captured child capabilities: %v", initResp.GetCapabilities())
				p.mutex.Lock()
				p.childCapabilities = initResp.GetCapabilities()
				p.childWorkerMetadata = initResp.GetWorkerMetadata()
				p.mutex.Unlock()
				close(p.childInitDone)
				continue
			}

			// Forward to Host
			p.sendToHost(msg)
		}
	}()

	// 8. Send FunctionEnvironmentReloadResponse after child is initialized
	<-p.childInitDone
	p.mutex.Lock()
	caps := p.childCapabilities
	meta := p.childWorkerMetadata
	p.mutex.Unlock()
	log.Printf("Child initialized - sending FunctionEnvironmentReloadResponse with capabilities: %v", caps)
	p.sendToHost(&pb.StreamingMessage{
		RequestId: p.savedReloadRequestId,
		Content: &pb.StreamingMessage_FunctionEnvironmentReloadResponse{
			FunctionEnvironmentReloadResponse: &pb.FunctionEnvironmentReloadResponse{
				Result:                     &pb.StatusResult{Status: pb.StatusResult_Success},
				Capabilities:               caps,
				CapabilitiesUpdateStrategy: pb.FunctionEnvironmentReloadResponse_replace,
				WorkerMetadata:             meta,
			},
		},
	})
}
