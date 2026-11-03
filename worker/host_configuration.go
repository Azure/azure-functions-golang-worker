package worker

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unicode/utf16"
	"unicode/utf8"

	pb "github.com/azure/azure-functions-golang-worker/worker/proto"
)

const maxHostConfigurationBytes = 65_536

var (
	errHostConfigurationMissing  = errors.New("host configuration is missing")
	errHostConfigurationTooLarge = errors.New("host configuration exceeds the delivery limit")
	errHostConfigurationInvalid  = errors.New("host configuration encoding is invalid")
	errHostConfigurationRoot     = errors.New("host configuration root is unavailable")
)

// A new dispatcher (including a reconnected Start) must not recapture the
// application's snapshot. App content is immutable for this process lifetime.
var processHostConfiguration = newHostConfigurationProvider(func() ([]byte, error) {
	return readHostConfiguration(os.Getenv("FUNCTIONS_APPLICATION_DIRECTORY"), openHostConfiguration)
})

type hostConfigurationSnapshot struct {
	status  pb.HostConfiguration_Status
	content string
}

func newHostConfigurationProvider(read func() ([]byte, error)) func(*pb.WorkerInitRequest) *pb.HostConfiguration {
	snapshot := sync.OnceValue(func() hostConfigurationSnapshot {
		content, err := read()
		switch {
		case errors.Is(err, errHostConfigurationMissing):
			return hostConfigurationSnapshot{status: pb.HostConfiguration_Missing}
		case errors.Is(err, errHostConfigurationTooLarge):
			return hostConfigurationSnapshot{status: pb.HostConfiguration_TooLarge}
		case err != nil:
			return hostConfigurationSnapshot{status: pb.HostConfiguration_Error}
		default:
			return hostConfigurationSnapshot{status: pb.HostConfiguration_Available, content: string(content)}
		}
	})
	return func(req *pb.WorkerInitRequest) *pb.HostConfiguration {
		if req.GetCapabilities()["HostConfiguration"] != "1" {
			return nil
		}
		captured := snapshot()
		response := &pb.HostConfiguration{Status: captured.status}
		if captured.status == pb.HostConfiguration_Available {
			response.Content = []byte(captured.content)
		}
		return response
	}
}

func openHostConfiguration(path string) (io.ReadCloser, error) {
	return os.Open(path)
}

func readHostConfiguration(root string, open func(string) (io.ReadCloser, error)) ([]byte, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errHostConfigurationRoot
	}

	file, err := open(filepath.Join(root, "host.json"))
	if err != nil {
		// ENOENT can name either the file or an unavailable parent. Only a
		// missing file in an accessible directory is a Missing outcome.
		if os.IsNotExist(err) {
			if dir, statErr := os.Stat(root); statErr == nil && dir.IsDir() {
				return nil, errHostConfigurationMissing
			}
		}
		return nil, err
	}

	content, readErr := decodeHostConfiguration(file)
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return content, nil
}

// Decode incrementally, bounding both buffered input and the final UTF-8 output.
// Validate scalars before encoding instead of replacing malformed UTF-16/32.
func decodeHostConfiguration(source io.Reader) ([]byte, error) {
	reader := bufio.NewReaderSize(source, 4096)
	prefix, err := reader.Peek(4)
	if err != nil && err != io.EOF {
		return nil, err
	}

	var order binary.ByteOrder
	width, bom := 1, 0
	switch {
	case bytes.HasPrefix(prefix, []byte{0xff, 0xfe, 0, 0}):
		order, width, bom = binary.LittleEndian, 4, 4
	case bytes.HasPrefix(prefix, []byte{0, 0, 0xfe, 0xff}):
		order, width, bom = binary.BigEndian, 4, 4
	case bytes.HasPrefix(prefix, []byte{0xef, 0xbb, 0xbf}):
		bom = 3
	case bytes.HasPrefix(prefix, []byte{0xff, 0xfe}):
		order, width, bom = binary.LittleEndian, 2, 2
	case bytes.HasPrefix(prefix, []byte{0xfe, 0xff}):
		order, width, bom = binary.BigEndian, 2, 2
	}
	if _, err := reader.Discard(bom); err != nil {
		return nil, err
	}

	content := make([]byte, 0, 4096)
	for {
		var scalar rune
		if width == 1 {
			var size int
			scalar, size, err = reader.ReadRune()
			if err == nil && scalar == utf8.RuneError && size == 1 {
				err = errHostConfigurationInvalid
			}
		} else {
			scalar, err = readHostConfigurationScalar(reader, order, width)
		}
		if err == io.EOF {
			return content, nil
		}
		if err != nil {
			return nil, err
		}
		if len(content)+utf8.RuneLen(scalar) > maxHostConfigurationBytes {
			return nil, errHostConfigurationTooLarge
		}
		content = utf8.AppendRune(content, scalar)
	}
}

func readHostConfigurationScalar(reader io.Reader, order binary.ByteOrder, width int) (rune, error) {
	var unit [4]byte
	if _, err := io.ReadFull(reader, unit[:width]); err != nil {
		return 0, err
	}
	if width == 4 {
		scalar := order.Uint32(unit[:])
		if scalar > utf8.MaxRune || !utf8.ValidRune(rune(scalar)) {
			return 0, errHostConfigurationInvalid
		}
		return rune(scalar), nil
	}

	first := rune(order.Uint16(unit[:2]))
	if first >= 0xdc00 && first <= 0xdfff {
		return 0, errHostConfigurationInvalid
	}
	if first < 0xd800 || first > 0xdbff {
		return first, nil
	}
	if _, err := io.ReadFull(reader, unit[:2]); err != nil {
		// EOF after a high surrogate is not a complete document.
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, err
	}
	second := rune(order.Uint16(unit[:2]))
	if second < 0xdc00 || second > 0xdfff {
		return 0, errHostConfigurationInvalid
	}
	return utf16.DecodeRune(first, second), nil
}
