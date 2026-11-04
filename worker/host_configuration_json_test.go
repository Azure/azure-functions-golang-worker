package worker

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/iotest"

	"github.com/azure/azure-functions-golang-worker/sdk"
	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
	"google.golang.org/protobuf/proto"
)

func TestNormalizeHostConfiguration(t *testing.T) {
	for _, tt := range []struct {
		name, input, want string
	}{
		{"empty object", "{}", "{}"},
		{"comments and commas", "// header\n{ \"version\": \"2.0\", /* inline */ \"logging\": { \"a\": [1, 2,], }, }", `{"version":"2.0","logging":{"a":[1,2]}}`},
		{"raw tokens", `{ "a": "x\u0041\n", "n": [1e5, -0.50, true, false, null, {}, [],], "e": {}, }`, `{"a":"x\u0041\n","n":[1e5,-0.50,true,false,null,{},[]],"e":{}}`},
		{"number precision", `{"numbers": [ -0, 1E+2, 1e999, 123456789012345678901234567890, 0.123456789012345678901234567890, -1.234567890123456789e-1234 ]}`, `{"numbers":[-0,1E+2,1e999,123456789012345678901234567890,0.123456789012345678901234567890,-1.234567890123456789e-1234]}`},
		{"duplicate keys", `{ "a": 1, "\u0061": 2, "a": 3, }`, `{"a":1,"\u0061":2,"a":3}`},
		{"raw surrogate escapes", `{"\uD800":"\udc00", "pair": "\uD83D\uDE00", "slash": "\/", "quote": "\"", "backslash": "\\"}`, `{"\uD800":"\udc00","pair":"\uD83D\uDE00","slash":"\/","quote":"\"","backslash":"\\"}`},
		{"comment markers in strings", `{"//":"/*not a comment*/", "url": "https://example.test/a//b", "punctuation": "[,]{,}",}`, `{"//":"/*not a comment*/","url":"https://example.test/a//b","punctuation":"[,]{,}"}`},
		{"comments around punctuation", `{/*a*/"b"/*c*/:/*d*/[/*e*/1/*f*/,/*g*/]/*h*/,/*i*/}`, `{"b":[1]}`},
		{"nested scalars", `{ "number": 1.2300e+999, "string": "scalar", "boolean": true, "null": null, }`, `{"number":1.2300e+999,"string":"scalar","boolean":true,"null":null}`},
		{"empty containers", `{"containers":[{},[],{/*a*/},[/*b*/],]}`, `{"containers":[{},[],{},[]]}`},
		{"comment line endings", "// header\r{\r\n\"x\":1,// field\r\n}", `{"x":1}`},
		{"Unicode", "{\"x\":\"\u00e9\u20ac\U0001f600\ufffd\u2028\u2029\"}", "{\"x\":\"\u00e9\u20ac\U0001f600\ufffd\u2028\u2029\"}"},
		{"Unicode block comments", "/*\u2028\u2029*/ {\"x\": 1} /*\u2029\u2028*/", `{"x":1}`},
		{"reversed surrogate escapes", `{"\uDC00\uD800":"\uDC00\uD800"}`, `{"\uDC00\uD800":"\uDC00\uD800"}`},
		{"depth 64", `{"a":` + strings.Repeat("[", 63) + "0" + strings.Repeat("]", 63) + "}", `{"a":` + strings.Repeat("[", 63) + "0" + strings.Repeat("]", 63) + "}"},
		{"object depth 64", strings.Repeat(`{"a":`, 64) + "0" + strings.Repeat("}", 64), strings.Repeat(`{"a":`, 64) + "0" + strings.Repeat("}", 64)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeHostConfiguration([]byte(tt.input))
			if err != nil || string(got) != tt.want {
				t.Fatalf("got %q, err = %v; want %q", got, err, tt.want)
			}
			if len(got) == 0 || got[0] != '{' || !json.Valid(got) {
				t.Fatal("normalized output is not a strict JSON object")
			}
		})
	}
}

func TestNormalizeHostConfigurationInvalid(t *testing.T) {
	for _, input := range []string{
		"", "[]", "[7]", `"s"`, "1", "null", "true", "false", "// c\n[1]", "/* c */ true",
		" ", "\t\r\n", "// comment", "/* comment */", "not json", "{", "[", `{"x":}`,
		"{,}", "[,]", "[1,,]", `{"a":,}`, `{"a":1,,}`, "[1, ,]", `{"a",}`, "[1,}",
		"{} {}", "1 true", "1,2", "{} garbage", "{} /* unterminated", "/*", "/", "[/* ]",
		"1/* comment */2", "t/* comment */rue", "[true/* comment */false]", `{"a"/* comment */"b"}`,
		"'single'", `"\x41"`, `"\u123"`, `"\uGGGG"`, `"unterminated`, "\"line\nbreak\"",
		"NaN", "Infinity", "-Infinity", "+1", "01", ".1", "1.", "1e", "1e+", "--1",
		"{\u00a0\"x\":1}", "// comment\u2028\n{}", "// comment\u2029\n{}",
		"// comment\u2028", "// comment\u2029", "{}// comment\u2028", "{}// comment\u2029",
		"[\u2028]", "{\u2029}", "true\u2028", "\u2029null",
		`{"a":` + strings.Repeat("[", 64) + "0" + strings.Repeat("]", 64) + "}",
		strings.Repeat(`{"a":`, 65) + "0" + strings.Repeat("}", 65),
	} {
		t.Run(input, func(t *testing.T) {
			got, err := normalizeHostConfiguration([]byte(input))
			if !errors.Is(err, errHostConfigurationJSON) || got != nil {
				t.Fatalf("invalid input returned %q, %v", got, err)
			}
		})
	}
}

func TestHostConfigurationNormalizationAcrossEncodings(t *testing.T) {
	const input = "// header\n{ \"unicode\": \"\ufffd\u00e9\U0001f600\", \"number\": 1e999, \"escape\": \"\\uD800\", }"
	const want = "{\"unicode\":\"\ufffd\u00e9\U0001f600\",\"number\":1e999,\"escape\":\"\\uD800\"}"
	for _, encoding := range hostConfigurationEncodings {
		t.Run(encoding, func(t *testing.T) {
			for _, text := range []string{"{}", input} {
				var stream *hostConfigurationTestStream
				content, err := readHostConfiguration("relative-root", func(string) (io.ReadCloser, error) {
					stream = &hostConfigurationTestStream{Reader: iotest.OneByteReader(bytes.NewReader(encodeHostConfigurationTest(text, encoding)))}
					return stream, nil
				})
				expected := want
				if text == "{}" {
					expected = "{}"
				}
				if err != nil || string(content) != expected || !stream.closed {
					t.Fatalf("got %q, err = %v, closed = %v", content, err, stream.closed)
				}
			}
		})
	}
}

func TestHostConfigurationNormalizedLimits(t *testing.T) {
	const prefix, suffix = `{"v":"`, `"}`
	const payloadBytes = maxHostConfigurationBytes - len(prefix) - len(suffix)
	for _, encoding := range hostConfigurationEncodings {
		t.Run(encoding, func(t *testing.T) {
			for _, scalar := range []string{"a", "\u20ac", "\U0001f600"} {
				payload := strings.Repeat(scalar, payloadBytes/len(scalar))
				payload += strings.Repeat("a", payloadBytes-len(payload))
				exact := prefix + payload + suffix
				for _, input := range []string{exact, prefix + payload + "a" + suffix, " " + exact, "//" + strings.Repeat(" ", maxHostConfigurationBytes) + "\n{}"} {
					content, err := readHostConfiguration("relative-root", func(string) (io.ReadCloser, error) {
						return io.NopCloser(bytes.NewReader(encodeHostConfigurationTest(input, encoding))), nil
					})
					if input == exact {
						if err != nil || string(content) != exact || len(content) != maxHostConfigurationBytes {
							t.Fatalf("exact converted and normalized limit: length %d, err = %v", len(content), err)
						}
					} else if !errors.Is(err, errHostConfigurationTooLarge) || content != nil {
						t.Fatalf("oversized input was parsed/compacted first: length %d, err = %v", len(content), err)
					}
				}
			}
		})
	}
	for _, input := range []string{strings.Repeat("x", maxHostConfigurationBytes+1), prefix + strings.Repeat("a", payloadBytes+1) + suffix} {
		if content, err := normalizeHostConfiguration([]byte(input)); !errors.Is(err, errHostConfigurationTooLarge) || content != nil {
			t.Fatalf("normalizer accepted oversized input: length %d, err = %v", len(content), err)
		}
	}
}

func TestHostConfigurationObjectRootDelivery(t *testing.T) {
	for _, tt := range []struct {
		name, input, want string
	}{
		{"empty or BOM-only", "", ""},
		{"array", "[]", ""},
		{"nonempty array", "[7]", ""},
		{"string", `"private-configuration"`, ""},
		{"number", "1", ""},
		{"null", "null", ""},
		{"true", "true", ""},
		{"false", "false", ""},
		{"commented array", "// private-configuration\n[1]", ""},
		{"commented scalar", "/* private-configuration */ true", ""},
		{"empty object", "{}", "{}"},
		{"commented object", "// comment\n{ \"values\": [1, \"text\", null, true, {},], }", `{"values":[1,"text",null,true,{}]}`},
	} {
		for _, encoding := range hostConfigurationEncodings {
			t.Run(tt.name+"/"+encoding, func(t *testing.T) {
				source := encodeHostConfigurationTest(tt.input, encoding)
				var logs bytes.Buffer
				var reads int
				disp := newTestDispatcher("object-root")
				disp.systemLogger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
				disp.hostConfigurationProvider = newHostConfigurationProvider(func() ([]byte, error) {
					reads++
					return readHostConfiguration("private-root", func(string) (io.ReadCloser, error) {
						return io.NopCloser(iotest.OneByteReader(bytes.NewReader(source))), nil
					})
				})
				baseline := handleWorkerInitRequest(&pb.WorkerInitRequest{}, "object-root", disp).GetWorkerInitResponse()
				if baseline.HostConfiguration != nil || reads != 0 || logs.Len() != 0 {
					t.Fatal("disabled request captured or logged configuration")
				}
				response := handleWorkerInitRequest(hostConfigurationRequest(), "object-root", disp).GetWorkerInitResponse()
				first := response.HostConfiguration
				wantStatus := pb.HostConfiguration_Available
				if tt.want == "" {
					wantStatus = pb.HostConfiguration_Error
				}
				if first == nil || first.Status != wantStatus || string(first.Content) != tt.want || reads != 1 {
					t.Fatalf("status/content/read count mismatch: %v, reads=%d", first, reads)
				}
				response.HostConfiguration = nil
				if !proto.Equal(response, baseline) || response.Result.Status != pb.StatusResult_Success {
					t.Fatal("document acceptance changed initialization results or capabilities")
				}
				if tt.want == "" {
					var record map[string]string
					if strings.Count(logs.String(), "\n") != 1 || strings.Contains(logs.String(), "private-") {
						t.Fatalf("expected one content-free warning, got %q", logs.String())
					}
					if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
						t.Fatal(err)
					}
					if record["level"] != "WARN" || record["reason"] != "InvalidJson" ||
						record["msg"] != "Host configuration could not be delivered to the host. Reason: InvalidJson. The failure is retained until the worker process restarts." {
						t.Fatalf("unexpected failure warning: %v", record)
					}
				} else if logs.Len() != 0 {
					t.Fatal("valid object emitted a failure warning")
				}
				before := logs.String()
				source = []byte(`{"later":true}`)
				replay := handleWorkerInitRequest(hostConfigurationRequest(), "object-root", disp).GetWorkerInitResponse().HostConfiguration
				if replay == first || !proto.Equal(replay, first) || reads != 1 || logs.String() != before {
					t.Fatal("replay recaptured, relogged or changed the cached outcome")
				}
			})
		}
	}
}

func TestHostConfigurationFailureReasons(t *testing.T) {
	for _, tt := range []struct {
		name     string
		root     string
		openErr  error
		reader   io.Reader
		closeErr error
		want     error
	}{
		{name: "unset root", root: " \t", want: errHostConfigurationRoot},
		{name: "missing parent", root: t.TempDir() + string(os.PathSeparator) + "absent", openErr: os.ErrNotExist, want: errHostConfigurationParent},
		{name: "missing file", root: t.TempDir(), openErr: os.ErrNotExist, want: errHostConfigurationMissing},
		{name: "not-directory with accessible root", root: t.TempDir(), openErr: syscall.ENOTDIR, want: errHostConfigurationOpen},
		{name: "open permission", root: t.TempDir(), openErr: os.ErrPermission, want: errHostConfigurationAccess},
		{name: "other open error", root: t.TempDir(), openErr: errors.New("secret open error"), want: errHostConfigurationOpen},
		{name: "not-found during read", root: t.TempDir(), reader: iotest.ErrReader(os.ErrNotExist), want: errHostConfigurationRead},
		{name: "not-found after partial read", root: t.TempDir(), reader: io.MultiReader(strings.NewReader("{}"), iotest.ErrReader(os.ErrNotExist)), want: errHostConfigurationRead},
		{name: "permission during read", root: t.TempDir(), reader: iotest.ErrReader(os.ErrPermission), want: errHostConfigurationRead},
		{name: "not-found during close", root: t.TempDir(), reader: strings.NewReader("{}"), closeErr: os.ErrNotExist, want: errHostConfigurationRead},
		{name: "invalid UTF-8", root: t.TempDir(), reader: bytes.NewReader([]byte{0xff}), want: errHostConfigurationInvalid},
		{name: "truncated UTF-16", root: t.TempDir(), reader: bytes.NewReader([]byte{0xff, 0xfe, 0x41}), want: errHostConfigurationInvalid},
		{name: "truncated UTF-32", root: t.TempDir(), reader: bytes.NewReader([]byte{0, 0, 0xfe, 0xff, 0}), want: errHostConfigurationInvalid},
		{name: "invalid JSON", root: t.TempDir(), reader: strings.NewReader("{secret"), want: errHostConfigurationJSON},
		{name: "whitespace JSON", root: t.TempDir(), reader: strings.NewReader(" \n"), want: errHostConfigurationJSON},
		{name: "oversized malformed JSON", root: t.TempDir(), reader: strings.NewReader(strings.Repeat("x", maxHostConfigurationBytes+1)), want: errHostConfigurationTooLarge},
	} {
		t.Run(tt.name, func(t *testing.T) {
			content, err := readHostConfiguration(tt.root, func(string) (io.ReadCloser, error) {
				if tt.openErr != nil {
					return nil, &os.PathError{Op: "open", Path: "private-path", Err: tt.openErr}
				}
				return &hostConfigurationTestStream{Reader: tt.reader, closeErr: tt.closeErr}, nil
			})
			if !errors.Is(err, tt.want) || content != nil {
				t.Fatalf("got %q, %v; want %v", content, err, tt.want)
			}
		})
	}
}

func TestHostConfigurationFailureWarning(t *testing.T) {
	for _, outcome := range []error{
		nil, errHostConfigurationMissing, errHostConfigurationRoot, errHostConfigurationParent,
		errHostConfigurationAccess, errHostConfigurationOpen, errHostConfigurationRead,
		errHostConfigurationInvalid, errHostConfigurationJSON, errHostConfigurationTooLarge,
		&os.PathError{Op: "read", Path: "private-path", Err: errors.New("private-configuration")},
	} {
		var logs bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logs, nil))
		var reads atomic.Int32
		provider := newHostConfigurationProvider(func() ([]byte, error) {
			reads.Add(1)
			return []byte("private-configuration"), outcome
		})
		for _, req := range []*pb.WorkerInitRequest{nil, {}, {Capabilities: map[string]string{"HostConfiguration": "true"}}, {Capabilities: map[string]string{"hostconfiguration": "1"}}} {
			if provider(req, logger) != nil || reads.Load() != 0 || logs.Len() != 0 {
				t.Fatal("disabled request performed acquisition or emitted diagnostics")
			}
		}

		var wg sync.WaitGroup
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				provider(hostConfigurationRequest(), logger)
			}()
		}
		wg.Wait()
		if reads.Load() != 1 {
			t.Fatalf("captured %d times", reads.Load())
		}
		before := logs.String()
		provider(hostConfigurationRequest(), logger)
		provider(&pb.WorkerInitRequest{}, logger)
		if logs.String() != before {
			t.Fatal("replay or disabled init logged again")
		}
		if outcome == nil || outcome == errHostConfigurationMissing {
			if logs.Len() != 0 {
				t.Fatal("Available or Missing logged a failure")
			}
			continue
		}
		if strings.Count(before, "\n") != 1 || strings.Contains(before, "private-") {
			t.Fatalf("expected one content-free warning, got %q", before)
		}
		var record map[string]string
		if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		reason := errHostConfigurationRead
		var failure hostConfigurationFailure
		if errors.As(outcome, &failure) {
			reason = failure
		}
		if record["level"] != "WARN" || record["reason"] != string(reason) ||
			record["msg"] != "Host configuration could not be delivered to the host. Reason: "+string(reason)+". The failure is retained until the worker process restarts." {
			t.Fatalf("unexpected warning: %v", record)
		}
	}
}

func TestHostConfigurationProcessFailureWarning(t *testing.T) {
	const childVariable = "GO_WORKER_HOST_CONFIGURATION_FAILURE_TEST"
	if os.Getenv(childVariable) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestHostConfigurationProcessFailureWarning$")
		cmd.Env = append(os.Environ(), childVariable+"=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("isolated process test failed: %v\n%s", err, output)
		}
		return
	}
	var logs bytes.Buffer
	app := sdk.FunctionApp()
	first := NewDispatcher(&WorkerStartupConfig{}, app)
	first.systemLogger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	root := t.TempDir()
	path := filepath.Join(root, "host.json")
	writeHostConfigurationTestFile(t, path, []byte("{private-configuration"))
	t.Setenv("FUNCTIONS_APPLICATION_DIRECTORY", root)
	for _, capability := range []map[string]string{nil, {"HostConfiguration": "true"}, {"hostconfiguration": "1"}} {
		request := &pb.WorkerInitRequest{Capabilities: capability}
		response := handleWorkerInitRequest(request, "disabled", first).GetWorkerInitResponse()
		if response.HostConfiguration != nil || logs.Len() != 0 {
			t.Fatal("ordinary Host init captured a snapshot or emitted a warning")
		}
	}
	response := handleWorkerInitRequest(hostConfigurationRequest(), "enabled", first).GetWorkerInitResponse()
	if response.Result.Status != pb.StatusResult_Success || response.HostConfiguration.Status != pb.HostConfiguration_Error ||
		len(response.HostConfiguration.Content) != 0 {
		t.Fatal("invalid JSON changed initialization success or carried content")
	}
	if strings.Count(logs.String(), "\n") != 1 || !strings.Contains(logs.String(), "InvalidJson") ||
		strings.Contains(logs.String(), root) || strings.Contains(logs.String(), "private-") {
		t.Fatalf("expected one safe failure warning, got %q", logs.String())
	}
	before := logs.String()
	writeHostConfigurationTestFile(t, path, []byte("{}"))
	second := NewDispatcher(&WorkerStartupConfig{}, app)
	second.systemLogger = first.systemLogger
	for _, disp := range []*Dispatcher{second, first, second} {
		replay := handleWorkerInitRequest(hostConfigurationRequest(), "replay", disp).GetWorkerInitResponse()
		if replay.HostConfiguration == response.HostConfiguration || replay.HostConfiguration.Status != pb.HostConfiguration_Error ||
			logs.String() != before || replay.Result.Status != pb.StatusResult_Success {
			t.Fatal("dispatcher replay recaptured, relogged, or failed worker initialization")
		}
	}
}
