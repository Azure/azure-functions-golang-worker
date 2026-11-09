package worker

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"unicode/utf16"

	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
	"google.golang.org/protobuf/proto"
)

func hostConfigurationRequest() *pb.WorkerInitRequest {
	return &pb.WorkerInitRequest{Capabilities: map[string]string{"HostConfiguration": "1"}}
}

func TestHostConfigurationGate(t *testing.T) {
	var reads int
	provider := newHostConfigurationProvider(func() ([]byte, error) {
		reads++
		return []byte("{}"), nil
	})
	requests := []*pb.WorkerInitRequest{
		nil,
		{},
		{Capabilities: map[string]string{"hostconfiguration": "1"}},
		{Capabilities: map[string]string{"HOSTCONFIGURATION": "1"}},
	}
	for _, value := range []string{"", "0", "2", "true", "True", "01", " 1", "1 ", "1\n"} {
		requests = append(requests, &pb.WorkerInitRequest{Capabilities: map[string]string{"HostConfiguration": value}})
	}
	for _, req := range requests {
		if got := provider(req, nil); got != nil {
			t.Fatalf("disabled request returned %v", got)
		}
	}
	if reads != 0 {
		t.Fatalf("disabled requests captured the snapshot %d times", reads)
	}
	if got := provider(hostConfigurationRequest(), nil); got.Status != pb.HostConfiguration_Available {
		t.Fatalf("enabled request status = %v", got.Status)
	}
	for _, req := range requests {
		if got := provider(req, nil); got != nil {
			t.Fatalf("disabled request returned cached snapshot %v", got)
		}
	}
	if reads != 1 {
		t.Fatalf("capture count = %d, want 1", reads)
	}
}

func TestHostConfigurationSnapshot(t *testing.T) {
	for _, tt := range []struct {
		name   string
		err    error
		status pb.HostConfiguration_Status
	}{
		{"available", nil, pb.HostConfiguration_Available},
		{"missing", errHostConfigurationMissing, pb.HostConfiguration_Missing},
		{"accessor failure", errors.New("private-root: accessor failure"), pb.HostConfiguration_Error},
		{"read failure", &os.PathError{Op: "read", Path: "private-path", Err: os.ErrPermission}, pb.HostConfiguration_Error},
		{"too large", errHostConfigurationTooLarge, pb.HostConfiguration_TooLarge},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var reads atomic.Int32
			source := []byte("private-configuration")
			provider := newHostConfigurationProvider(func() ([]byte, error) {
				reads.Add(1)
				return source, tt.err
			})
			const callers = 32
			results := make([]*pb.HostConfiguration, callers)
			var wg sync.WaitGroup
			for i := range results {
				wg.Add(1)
				go func() {
					defer wg.Done()
					results[i] = provider(hostConfigurationRequest(), nil)
				}()
			}
			wg.Wait()
			if reads.Load() != 1 {
				t.Fatalf("capture count = %d, want 1", reads.Load())
			}
			source[0] = 'X'
			for i, result := range results {
				if result.Status != tt.status {
					t.Fatalf("response %d status = %v, want %v", i, result.Status, tt.status)
				}
				if i > 0 && result == results[0] {
					t.Fatal("responses share an envelope")
				}
				if tt.status == pb.HostConfiguration_Available {
					if string(result.Content) != "private-configuration" {
						t.Fatalf("response %d content was mutated", i)
					}
					result.Content[0] = 'Y'
				} else if len(result.Content) != 0 {
					t.Fatalf("failure response carries content: %v", result.Status)
				}
				result.Status = pb.HostConfiguration_Unspecified
			}
			again := provider(hostConfigurationRequest(), nil)
			if again.Status != tt.status {
				t.Fatal("response mutation changed cached status")
			}
			if tt.status == pb.HostConfiguration_Available && string(again.Content) != "private-configuration" {
				t.Fatal("response mutation changed cached content")
			}
		})
	}
}

func TestHostConfigurationApplicationDirectory(t *testing.T) {
	for _, tt := range []struct {
		name  string
		root  string
		unset bool
	}{
		{name: "unset", unset: true},
		{name: "empty"},
		{name: "whitespace", root: " \t\r\n\u2003"},
		{name: "explicit", root: "/configured-root"},
		{name: "relative", root: "relative-root"},
		{name: "untrimmed", root: " /configured-root "},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("FUNCTIONS_APPLICATION_DIRECTORY", "/ignored-internal-root")
			t.Setenv("AzureWebJobsScriptRoot", tt.root)
			if tt.unset {
				if err := os.Unsetenv("AzureWebJobsScriptRoot"); err != nil {
					t.Fatal(err)
				}
			}
			wantRoot := tt.root
			if strings.TrimSpace(wantRoot) == "" && runtime.GOOS == "linux" {
				wantRoot = "/home/site/wwwroot"
			}
			content, err := readHostConfiguration(hostConfigurationAppDirectory(), func(path string) (io.ReadCloser, error) {
				if strings.TrimSpace(wantRoot) == "" {
					t.Fatal("unset root reached open on a non-Linux platform")
				}
				if want := filepath.Join(wantRoot, "host.json"); path != want {
					t.Fatalf("path = %q, want %q", path, want)
				}
				return io.NopCloser(strings.NewReader(`{"root":"selected"}`)), nil
			})
			if strings.TrimSpace(wantRoot) == "" {
				if !errors.Is(err, errHostConfigurationRoot) {
					t.Fatalf("error = %v, want AppDirectoryNotSet", err)
				}
			} else if err != nil || string(content) != `{"root":"selected"}` {
				t.Fatalf("content = %q, err = %v", content, err)
			}
		})
	}
}

func TestHostConfigurationRead(t *testing.T) {
	t.Run("invalid roots do not open", func(t *testing.T) {
		for _, root := range []string{"", " ", "\r\n\t", "\u2003"} {
			_, err := readHostConfiguration(root, func(string) (io.ReadCloser, error) {
				t.Fatal("invalid root reached open")
				return nil, nil
			})
			if !errors.Is(err, errHostConfigurationRoot) {
				t.Fatalf("root %q: error = %v", root, err)
			}
		}
	})
	t.Run("missing file", func(t *testing.T) {
		_, err := readHostConfiguration(t.TempDir(), openHostConfiguration)
		if !errors.Is(err, errHostConfigurationMissing) {
			t.Fatalf("error = %v, want Missing", err)
		}
	})
	t.Run("missing parent", func(t *testing.T) {
		_, err := readHostConfiguration(filepath.Join(t.TempDir(), "absent", "nested"), openHostConfiguration)
		if err == nil || errors.Is(err, errHostConfigurationMissing) {
			t.Fatalf("unavailable parent must be Error, got %v", err)
		}
	})
	t.Run("root is a file", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "file")
		writeHostConfigurationTestFile(t, root, []byte("{}"))
		_, err := readHostConfiguration(root, openHostConfiguration)
		if err == nil || errors.Is(err, errHostConfigurationMissing) {
			t.Fatalf("file root must be Error, got %v", err)
		}
	})
	t.Run("host.json is a directory", func(t *testing.T) {
		root := t.TempDir()
		if err := os.Mkdir(filepath.Join(root, "host.json"), 0700); err != nil {
			t.Fatal(err)
		}
		if _, err := readHostConfiguration(root, openHostConfiguration); err == nil {
			t.Fatal("directory must be Error")
		}
	})
	t.Run("permission denied", func(t *testing.T) {
		_, err := readHostConfiguration(t.TempDir(), func(path string) (io.ReadCloser, error) {
			return nil, &os.PathError{Op: "open", Path: path, Err: os.ErrPermission}
		})
		if !errors.Is(err, errHostConfigurationAccess) {
			t.Fatalf("error = %v, want permission failure", err)
		}
	})
	t.Run("invalid path", func(t *testing.T) {
		_, err := readHostConfiguration("invalid\x00root", openHostConfiguration)
		if err == nil || errors.Is(err, errHostConfigurationMissing) {
			t.Fatalf("invalid path must be Error, got %v", err)
		}
	})
	t.Run("native relative root", func(t *testing.T) {
		root := t.TempDir()
		t.Chdir(filepath.Dir(root))
		writeHostConfigurationTestFile(t, filepath.Join(root, "host.json"), []byte(`{ "relative": true, }`))
		content, err := readHostConfiguration(filepath.Base(root), openHostConfiguration)
		if err != nil || string(content) != `{"relative":true}` {
			t.Fatalf("relative root: content = %q, err = %v", content, err)
		}
	})
	t.Run("no successful directory preflight", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "nonexistent")
		content, err := readHostConfiguration(root, func(path string) (io.ReadCloser, error) {
			if path != filepath.Join(root, "host.json") {
				t.Fatalf("path = %q", path)
			}
			return io.NopCloser(strings.NewReader("{}")), nil
		})
		if err != nil || string(content) != "{}" {
			t.Fatalf("unexpected preflight: content = %q, err = %v", content, err)
		}
	})
	t.Run("read and close failures", func(t *testing.T) {
		for _, readFailure := range []bool{false, true} {
			stream := &hostConfigurationTestStream{Reader: strings.NewReader("{}"), closeErr: errors.New("private close path")}
			if readFailure {
				stream.Reader = io.MultiReader(strings.NewReader("{}"), iotest.ErrReader(errors.New("private read path")))
			}
			content, err := readHostConfiguration(t.TempDir(), func(string) (io.ReadCloser, error) { return stream, nil })
			if err == nil || content != nil || !stream.closed {
				t.Fatalf("read failure %v: content = %q, err = %v, closed = %v", readFailure, content, err, stream.closed)
			}
		}
	})
}

type hostConfigurationTestStream struct {
	io.Reader
	closeErr error
	closed   bool
}

func (s *hostConfigurationTestStream) Close() error {
	s.closed = true
	return s.closeErr
}

var hostConfigurationEncodings = []string{"UTF-8", "UTF-8 BOM", "UTF-16LE", "UTF-16BE", "UTF-32LE", "UTF-32BE"}

func encodeHostConfigurationTest(text, encoding string) []byte {
	if encoding == "UTF-8" {
		return []byte(text)
	}
	if encoding == "UTF-8 BOM" {
		return append([]byte{0xef, 0xbb, 0xbf}, text...)
	}
	var order binary.ByteOrder = binary.LittleEndian
	if strings.HasSuffix(encoding, "BE") {
		order = binary.BigEndian
	}
	var content []byte
	var unit [4]byte
	if strings.HasPrefix(encoding, "UTF-16") {
		for _, value := range utf16.Encode([]rune("\ufeff" + text)) {
			order.PutUint16(unit[:2], value)
			content = append(content, unit[:2]...)
		}
	} else {
		for _, value := range "\ufeff" + text {
			order.PutUint32(unit[:], uint32(value))
			content = append(content, unit[:]...)
		}
	}
	return content
}

func TestHostConfigurationDecode(t *testing.T) {
	for _, encoding := range hostConfigurationEncodings {
		t.Run(encoding, func(t *testing.T) {
			for _, text := range []string{"", "{}", "{invalid JSON", "a\x00\u00e9\u4e2d\U0001f642\ufffd\ufeffz", strings.Repeat("a", 4095) + "\U0001f642"} {
				source := encodeHostConfigurationTest(text, encoding)
				for _, reader := range []io.Reader{bytes.NewReader(source), iotest.OneByteReader(bytes.NewReader(source))} {
					got, err := decodeHostConfiguration(reader)
					if err != nil || string(got) != text {
						t.Fatalf("input length %d: output length = %d, err = %v", len(source), len(got), err)
					}
				}
			}
		})
	}
}

func TestHostConfigurationMalformedEncoding(t *testing.T) {
	for name, source := range map[string][]byte{
		"UTF-8 invalid":         {0xff},
		"UTF-8 truncated BOM":   {0xef, 0xbb},
		"UTF-8 truncated":       {'a', 0xe2, 0x82},
		"UTF-8 overlong":        {0xc0, 0xaf},
		"UTF-8 surrogate":       {0xed, 0xa0, 0x80},
		"UTF-8 above max":       {0xf4, 0x90, 0x80, 0x80},
		"UTF-16LE odd":          {0xff, 0xfe, 0x41},
		"UTF-16BE odd":          {0xfe, 0xff, 0},
		"UTF-16LE high":         {0xff, 0xfe, 0, 0xd8},
		"UTF-16BE high":         {0xfe, 0xff, 0xd8, 0},
		"UTF-16LE low":          {0xff, 0xfe, 0, 0xdc},
		"UTF-16BE low":          {0xfe, 0xff, 0xdc, 0},
		"UTF-16LE bad pair":     {0xff, 0xfe, 0, 0xd8, 0x41, 0},
		"UTF-16BE bad pair":     {0xfe, 0xff, 0xd8, 0, 0, 0x41},
		"UTF-16LE partial pair": {0xff, 0xfe, 0, 0xd8, 0},
		"UTF-32LE partial":      {0xff, 0xfe, 0, 0, 0x41, 0, 0},
		"UTF-32BE partial":      {0, 0, 0xfe, 0xff, 0},
		"UTF-32LE surrogate":    {0xff, 0xfe, 0, 0, 0, 0xd8, 0, 0},
		"UTF-32BE surrogate":    {0, 0, 0xfe, 0xff, 0, 0, 0xd8, 0},
		"UTF-32LE above max":    {0xff, 0xfe, 0, 0, 0, 0, 0x11, 0},
		"UTF-32BE above max":    {0, 0, 0xfe, 0xff, 0, 0x11, 0, 0},
		"UTF-32 unsigned":       {0xff, 0xfe, 0, 0, 0xff, 0xff, 0xff, 0xff},
	} {
		t.Run(name, func(t *testing.T) {
			for _, reader := range []io.Reader{bytes.NewReader(source), iotest.OneByteReader(bytes.NewReader(source))} {
				if content, err := decodeHostConfiguration(reader); !errors.Is(err, errHostConfigurationInvalid) || content != nil {
					t.Fatalf("malformed source returned %q, %v", content, err)
				}
			}
		})
	}
}

func TestHostConfigurationOutputLimit(t *testing.T) {
	for _, encoding := range hostConfigurationEncodings {
		t.Run(encoding, func(t *testing.T) {
			for _, text := range []string{
				strings.Repeat("a", maxHostConfigurationBytes),
				strings.Repeat("\u4e2d", 21845) + "a",
				strings.Repeat("\U0001f642", 16384),
			} {
				for _, suffix := range []string{"", "a"} {
					input := text + suffix
					got, err := decodeHostConfiguration(bytes.NewReader(encodeHostConfigurationTest(input, encoding)))
					if suffix == "" {
						if err != nil || len(got) != maxHostConfigurationBytes || string(got) != input {
							t.Fatalf("65,536 bytes: got length %d, err = %v", len(got), err)
						}
					} else if !errors.Is(err, errHostConfigurationTooLarge) || got != nil {
						t.Fatalf("65,537 bytes: got length %d, err = %v", len(got), err)
					}
				}
			}
		})
	}
	t.Run("does not drain unbounded input", func(t *testing.T) {
		reader := &hostConfigurationCountingReader{}
		content, err := decodeHostConfiguration(reader)
		if !errors.Is(err, errHostConfigurationTooLarge) || content != nil {
			t.Fatalf("content = %q, err = %v", content, err)
		}
		if reader.read > maxHostConfigurationBytes+4096 {
			t.Fatalf("read %d bytes before rejecting", reader.read)
		}
	})
	t.Run("malformed tail at exact limit", func(t *testing.T) {
		for _, encoding := range hostConfigurationEncodings {
			source := encodeHostConfigurationTest(strings.Repeat("a", maxHostConfigurationBytes), encoding)
			source = append(source, 0xff)
			content, err := decodeHostConfiguration(bytes.NewReader(source))
			if err == nil || errors.Is(err, errHostConfigurationTooLarge) || content != nil {
				t.Fatalf("%s: malformed trailing sequence returned length %d, err = %v", encoding, len(content), err)
			}
		}
	})
}

type hostConfigurationCountingReader struct{ read int }

func (r *hostConfigurationCountingReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'a'
	}
	r.read += len(p)
	return len(p), nil
}

func TestHostConfigurationCachedFileOutcomes(t *testing.T) {
	for _, initial := range []string{"available", "missing", "error", "invalid JSON", "too large"} {
		t.Run(initial, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "host.json")
			switch initial {
			case "available":
				writeHostConfigurationTestFile(t, path, []byte("{}"))
			case "error":
				writeHostConfigurationTestFile(t, path, []byte{0xff})
			case "invalid JSON":
				writeHostConfigurationTestFile(t, path, []byte("{"))
			case "too large":
				writeHostConfigurationTestFile(t, path, bytes.Repeat([]byte("a"), maxHostConfigurationBytes+1))
			}
			provider := newHostConfigurationProvider(func() ([]byte, error) {
				return readHostConfiguration(root, openHostConfiguration)
			})
			first := provider(hostConfigurationRequest(), nil)
			writeHostConfigurationTestFile(t, path, []byte(`{"changed":true}`))
			if again := provider(hostConfigurationRequest(), nil); !proto.Equal(first, again) {
				t.Fatal("file mutation changed the cached outcome")
			}
		})
	}
}

func TestHostConfigurationInitialization(t *testing.T) {
	for _, readErr := range []error{nil, errHostConfigurationMissing, errHostConfigurationTooLarge, errHostConfigurationJSON, errors.New("private-path: private-configuration")} {
		disp := newTestDispatcher("host-config-request")
		disp.App.Use(&capProviderMW{caps: map[string]string{
			"WorkerOpenTelemetryEnabled": "true",
			"TypedDataCollection":        "middleware-wins",
		}})
		disp.HTTPProxy = &httpProxy{url: "http://127.0.0.1:12345"}
		var logs bytes.Buffer
		disp.systemLogger = slog.New(slog.NewTextHandler(&logs, nil))
		disp.hostConfigurationProvider = newHostConfigurationProvider(func() ([]byte, error) {
			return []byte("private-configuration"), readErr
		})
		baseline := handleWorkerInitRequest(&pb.WorkerInitRequest{}, "host-config-request", disp).GetWorkerInitResponse()
		req := hostConfigurationRequest()
		req.FunctionAppDirectory = "ignored-host-root"
		req.WorkerDirectory = "ignored-worker-root"
		msg, err := disp.processRequestMessage(&pb.StreamingMessage{
			Content: &pb.StreamingMessage_WorkerInitRequest{WorkerInitRequest: req},
		})
		if err != nil {
			t.Fatal(err)
		}
		got := msg.GetWorkerInitResponse()
		if got.HostConfiguration == nil {
			t.Fatal("requested configuration is absent")
		}
		got.HostConfiguration = nil
		if !proto.Equal(got, baseline) {
			t.Fatal("host configuration changed the existing initialization response")
		}
		if got.Capabilities["TypedDataCollection"] != "middleware-wins" || got.Capabilities["WorkerOpenTelemetryEnabled"] != "true" ||
			got.Capabilities["HttpUri"] != disp.HTTPProxy.url || got.Capabilities["RequiresRouteParameters"] != "true" {
			t.Fatal("existing capabilities were lost")
		}
		if strings.Contains(got.String()+logs.String(), "private-") {
			t.Fatal("configuration content or path-bearing error leaked into result, metadata, capabilities, or logs")
		}
	}
}

func TestHostConfigurationConcurrentInitialization(t *testing.T) {
	var reads atomic.Int32
	disp := newTestDispatcher("concurrent")
	disp.hostConfigurationProvider = newHostConfigurationProvider(func() ([]byte, error) {
		reads.Add(1)
		return []byte("{}"), nil
	})
	const callers = 32
	results := make([]*pb.WorkerInitResponse, callers)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg, err := disp.processRequestMessage(&pb.StreamingMessage{
				Content: &pb.StreamingMessage_WorkerInitRequest{WorkerInitRequest: hostConfigurationRequest()},
			})
			if err != nil {
				t.Error(err)
				return
			}
			results[i] = msg.GetWorkerInitResponse()
		}()
	}
	wg.Wait()
	if reads.Load() != 1 {
		t.Fatalf("concurrent init captured %d snapshots", reads.Load())
	}
	for _, response := range results {
		if response == nil || response.Result.Status != pb.StatusResult_Success ||
			response.HostConfiguration.Status != pb.HostConfiguration_Available ||
			string(response.HostConfiguration.Content) != "{}" {
			t.Fatal("concurrent init did not return an independent successful snapshot")
		}
		response.HostConfiguration.Content[0] = 'X'
	}
}

func TestHostConfigurationProcessSnapshot(t *testing.T) {
	const childVariable = "GO_WORKER_HOST_CONFIGURATION_TEST"
	if os.Getenv(childVariable) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestHostConfigurationProcessSnapshot$")
		cmd.Env = append(os.Environ(), childVariable+"=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("isolated process test failed: %v\n%s", err, output)
		}
		return
	}
	root := t.TempDir()
	other := t.TempDir()
	writeHostConfigurationTestFile(t, filepath.Join(root, "host.json"), []byte(`{"root":"worker"}`))
	writeHostConfigurationTestFile(t, filepath.Join(other, "host.json"), []byte(`{"root":"host"}`))
	t.Setenv("AzureWebJobsScriptRoot", "")
	app := sdk.FunctionApp()
	firstDispatcher := NewDispatcher(&WorkerStartupConfig{}, app)
	if got := handleWorkerInitRequest(&pb.WorkerInitRequest{}, "off", firstDispatcher).GetWorkerInitResponse(); got.HostConfiguration != nil {
		t.Fatal("unrequested init returned a snapshot")
	}
	t.Setenv("AzureWebJobsScriptRoot", root)
	req := hostConfigurationRequest()
	req.FunctionAppDirectory, req.WorkerDirectory = other, other
	first := handleWorkerInitRequest(req, "first", firstDispatcher).GetWorkerInitResponse().HostConfiguration
	if first.Status != pb.HostConfiguration_Available || string(first.Content) != `{"root":"worker"}` {
		t.Fatalf("did not use the worker-local environment root: %v", first.Status)
	}
	t.Setenv("AzureWebJobsScriptRoot", other)
	writeHostConfigurationTestFile(t, filepath.Join(root, "host.json"), []byte(`{"changed":true}`))
	reconnected := NewDispatcher(&WorkerStartupConfig{}, app)
	second := handleWorkerInitRequest(req, "second", reconnected).GetWorkerInitResponse().HostConfiguration
	if first == second || !proto.Equal(first, second) {
		t.Fatal("new dispatcher did not replay the process snapshot in a fresh envelope")
	}
	first.Content[0] = 'X'
	if second.Content[0] != '{' {
		t.Fatal("dispatchers share mutable response bytes")
	}
}

func TestHostConfigurationProtocol(t *testing.T) {
	field := (&pb.WorkerInitResponse{}).ProtoReflect().Descriptor().Fields().ByName("host_configuration")
	if field == nil || field.Number() != 6 || field.Message().FullName() != "AzureFunctionsRpcMessages.HostConfiguration" {
		t.Fatal("WorkerInitResponse does not use canonical field 6")
	}
	fields := (&pb.HostConfiguration{}).ProtoReflect().Descriptor().Fields()
	if fields.ByName("status").Number() != 1 || fields.ByName("content").Number() != 2 {
		t.Fatal("HostConfiguration does not use canonical field numbers")
	}
	if !reflect.DeepEqual(pb.HostConfiguration_Status_value, map[string]int32{
		"Unspecified": 0, "Available": 1, "Missing": 2, "Error": 3, "TooLarge": 4,
	}) {
		t.Fatal("HostConfiguration does not use canonical status values")
	}
	for _, content := range [][]byte{nil, []byte("{malformed JSON")} {
		envelope := &pb.WorkerInitResponse{HostConfiguration: &pb.HostConfiguration{Status: pb.HostConfiguration_Available, Content: content}}
		wire, err := proto.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		var decoded pb.WorkerInitResponse
		if err := proto.Unmarshal(wire, &decoded); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(envelope, &decoded) || decoded.HostConfiguration == nil {
			t.Fatal("wire roundtrip lost Available content or presence")
		}
	}
}

func writeHostConfigurationTestFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
}
