package durabletask

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// startManagementStub checks one RPC's fields and returns a fixed reply. It
// never executes user code, stores instance state, or creates replay history.
func startManagementStub(t *testing.T, method string, want map[string]any, reply string) string {
	t.Helper()
	desc, err := protoregistry.GlobalFiles.FindDescriptorByName("TaskHubSidecarService")
	if err != nil {
		t.Fatal(err)
	}
	md := desc.(protoreflect.ServiceDescriptor).Methods().ByName(protoreflect.Name(method))
	if md == nil {
		t.Fatalf("unknown sidecar method %s", method)
	}
	response := newProto(t, md.Output().FullName())
	if err := protojson.Unmarshal([]byte(reply), response); err != nil {
		t.Fatalf("invalid fixed reply: %v", err)
	}
	var calls atomic.Int32
	server := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		full, _ := grpc.MethodFromServerStream(stream)
		if full != "/TaskHubSidecarService/"+method {
			t.Errorf("unexpected RPC %s", full)
			return status.Error(codes.Unimplemented, "unexpected RPC")
		}
		request := md.Input()
		mt, err := protoregistry.GlobalTypes.FindMessageByName(request.FullName())
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		msg := mt.New().Interface()
		if err := stream.RecvMsg(msg); err != nil {
			return err
		}
		raw, err := protojson.Marshal(msg)
		if err != nil {
			return err
		}
		var got map[string]any
		if err := json.Unmarshal(raw, &got); err != nil {
			return err
		}
		for key, value := range want {
			if !reflect.DeepEqual(got[key], value) {
				t.Errorf("%s request %s = %#v, want %#v", method, key, got[key], value)
			}
		}
		calls.Add(1)
		return stream.SendMsg(response)
	}))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		if err := server.Serve(lis); err != nil {
			t.Errorf("serve management stub: %v", err)
		}
	}()
	t.Cleanup(func() {
		server.Stop()
		if calls.Load() != 1 {
			t.Errorf("%s calls = %d, want 1", method, calls.Load())
		}
	})
	return lis.Addr().String()
}

func TestClient_FetchMetadata_NotFound(t *testing.T) {
	addr := startManagementStub(t, "GetInstance", map[string]any{
		"instanceId": "does-not-exist", "getInputsAndOutputs": true,
	}, `{"exists":false}`)
	client, err := Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.FetchOrchestrationMetadata(ctx, "does-not-exist")
	if !errors.Is(err, api.ErrInstanceNotFound) {
		t.Fatalf("err = %v, want api.ErrInstanceNotFound", err)
	}
}
