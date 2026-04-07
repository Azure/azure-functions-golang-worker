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
	"runtime"
	"sync"

	"github.com/azure/azure-functions-golang-worker/worker"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Proxy struct {
	pb.UnimplementedFunctionRpcServer

	hostClient pb.FunctionRpcClient
	hostStream grpc.BidiStreamingClient[pb.StreamingMessage, pb.StreamingMessage]

	childStream    pb.FunctionRpc_EventStreamServer
	childConnected chan struct{}

	config   *worker.WorkerStartupConfig
	server   *grpc.Server
	listener net.Listener

	savedInitRequest *pb.StreamingMessage
	mutex            sync.Mutex
	isSpecializing   bool
}

func main() {
	log.Println("Go Worker Proxy Starting...")

	// 1. Parse Args (same as Worker)
	config, err := worker.GetWorkerStartupConfig()
	if err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	p := &Proxy{
		config:         config,
		childConnected: make(chan struct{}),
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

	// 4. Check for Placeholder Mode vs Dedicated
	isPlaceholder := os.Getenv("WEBSITE_PLACEHOLDER_MODE") == "1"
	if !isPlaceholder {
		log.Println("Not in Placeholder Mode - Starting Worker immediately")
		p.mutex.Lock()
		p.isSpecializing = true
		p.mutex.Unlock()
		go p.specialize(nil) // nil means Use Default Environment
	}

	// 5. Start Main Loop
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
	return p.hostStream.Send(&pb.StreamingMessage{
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
			log.Fatalf("Local server failed: %v", err)
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

	close(p.childConnected)

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

	// If we're specializing (dedicated mode or env reload in progress),
	// wait for the child to connect before forwarding.
	if specializing {
		log.Printf("Specializing - waiting for child before forwarding: %T", msg.Content)
		<-p.childConnected
		p.mutex.Lock()
		p.childStream.Send(msg)
		p.mutex.Unlock()
		return
	}

	// Placeholder Mode Logic
	switch content := msg.Content.(type) {

	case *pb.StreamingMessage_WorkerInitRequest:
		log.Println("Received WorkerInitRequest from Host")
		p.savedInitRequest = msg

		// Respond to Host
		response := &pb.StreamingMessage{
			RequestId: msg.RequestId,
			Content: &pb.StreamingMessage_WorkerInitResponse{
				WorkerInitResponse: &pb.WorkerInitResponse{
					Result: &pb.StatusResult{Status: pb.StatusResult_Success},
					// TODO: Populate capabilities matching real worker
					Capabilities: map[string]string{
						"TypedDataCollection":               "true",
						"WorkerStatus":                      "true",
						"RpcHttpBodyOnly":                   "true",
						"RawHttpBodyBytes":                  "true",
						"RpcHTTPTriggerMetadataRemoved":     "true",
						"UseNullableValueDictionaryForHttp": "true",
						"HandlesWorkerTermination":          "true",
					},
					WorkerVersion: "1.0.0", // TODO: Get from build
				},
			},
		}
		p.hostStream.Send(response)
		p.sendHeartbeat() // Start failing heartbeat?

	case *pb.StreamingMessage_WorkerHeartbeat:
		p.hostStream.Send(&pb.StreamingMessage{
			RequestId: msg.RequestId,
			Content: &pb.StreamingMessage_WorkerHeartbeat{
				WorkerHeartbeat: &pb.WorkerHeartbeat{},
			},
		})

	case *pb.StreamingMessage_FunctionEnvironmentReloadRequest:
		log.Println("Received FunctionEnvironmentReloadRequest - SPECIALIZING")
		go p.specialize(content.FunctionEnvironmentReloadRequest)

	case *pb.StreamingMessage_FunctionsMetadataRequest:
		log.Println("Received FunctionsMetadataRequest in placeholder mode - returning empty")
		p.hostStream.Send(&pb.StreamingMessage{
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
		p.hostStream.Send(&pb.StreamingMessage{
			RequestId: msg.RequestId,
			Content: &pb.StreamingMessage_WorkerStatusResponse{
				WorkerStatusResponse: &pb.WorkerStatusResponse{},
			},
		})

	default:
		log.Printf("Unhandled message in placeholder mode: %T", msg.Content)
	}
}

func (p *Proxy) specialize(req *pb.FunctionEnvironmentReloadRequest) {
	// Guard: prevent double-specialization (only relevant for placeholder mode
	// where FunctionEnvironmentReloadRequest could theoretically arrive twice).
	// In dedicated mode, isSpecializing is pre-set by main() so we skip this check.
	if req != nil {
		p.mutex.Lock()
		if p.isSpecializing {
			p.mutex.Unlock()
			return
		}
		p.isSpecializing = true
		p.mutex.Unlock()
	}

	// 1. Prepare Environment and Dir
	var env []string
	var dir string

	if req != nil {
		env = os.Environ()
		for k, v := range req.EnvironmentVariables {
			env = append(env, fmt.Sprintf("%s=%s", k, v))
		}
		dir = req.FunctionAppDirectory
	} else {
		// Default / Dedicated Mode
		env = os.Environ()
		dir, _ = os.Getwd()
	}

	// 2. Determine App Path
	workerPath := "app"
	if runtime.GOOS == "windows" {
		workerPath = "app.exe"
	}
	if dir != "" {
		workerPath = filepath.Join(dir, workerPath)
	}

	// 3. Construct Args
	// We need to point the child to the PROXY, not the Host.
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
	cmd.Env = env
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

	// 5. Wait for Connection
	log.Println("Waiting for child to connect...")
	<-p.childConnected
	log.Println("Child Connected! Starting Bridge...")

	// 6. Bridge: Handshake
	// We must send the Saved WorkerInitRequest to the Child
	if p.savedInitRequest != nil {
		log.Println("Replaying WorkerInitRequest to Child")
		p.childStream.Send(p.savedInitRequest)
	}

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
				if p.savedInitRequest != nil {
					// Placeholder mode: proxy already responded to the host
					log.Println("Dropping WorkerInitResponse from Child (proxy already responded)")
					continue
				}
				// Dedicated mode: proxy didn't respond, forward child's response
				log.Println("Forwarding WorkerInitResponse from Child to Host")
			}

			// Forward to Host
			p.hostStream.Send(msg)
		}
	}()
}

func (p *Proxy) sendHeartbeat() {
	// Simple ticker to keep connection alive if needed
}
