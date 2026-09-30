package worker

import (
	"errors"
	"io/fs"
	"math"
	"testing"
	"time"
)

type binaryFileInfo struct {
	size int64
	mode fs.FileMode
}

func (f binaryFileInfo) Name() string       { return "app" }
func (f binaryFileInfo) Size() int64        { return f.size }
func (f binaryFileInfo) Mode() fs.FileMode  { return f.mode }
func (f binaryFileInfo) ModTime() time.Time { return time.Time{} }
func (f binaryFileInfo) IsDir() bool        { return f.mode.IsDir() }
func (f binaryFileInfo) Sys() any           { return nil }

func TestAppBinarySize(t *testing.T) {
	for _, tc := range []struct {
		name    string
		goos    string
		info    fs.FileInfo
		statErr error
		exeErr  error
		want    string
	}{
		{name: "linux executing file", goos: "linux", info: binaryFileInfo{size: 123}, want: "123"},
		{name: "windows executable path", goos: "windows", info: binaryFileInfo{size: 456}, want: "456"},
		{name: "darwin executable path", goos: "darwin", info: binaryFileInfo{size: 789}, want: "789"},
		{name: "64-bit length", goos: "linux", info: binaryFileInfo{size: math.MaxInt64}, want: "9223372036854775807"},
		{name: "zero length is not failure", goos: "linux", info: binaryFileInfo{}, want: "0"},
		{name: "procfs unavailable", goos: "linux", statErr: fs.ErrNotExist},
		{name: "access denied", goos: "windows", statErr: fs.ErrPermission},
		{name: "executable unavailable", goos: "windows", exeErr: errors.New("private path")},
		{name: "directory", goos: "linux", info: binaryFileInfo{size: 4096, mode: fs.ModeDir}},
		{name: "invalid length", goos: "linux", info: binaryFileInfo{size: -1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exeCalls, statCalls := 0, 0
			got := appBinarySizeWith(tc.goos, func() (string, error) {
				exeCalls++
				return "/private/application", tc.exeErr
			}, func(path string) (fs.FileInfo, error) {
				statCalls++
				wantPath := "/private/application"
				if tc.goos == "linux" {
					wantPath = "/proc/self/exe"
				}
				if path != wantPath {
					t.Fatalf("stat path = %q, want %q", path, wantPath)
				}
				return tc.info, tc.statErr
			})
			if got != tc.want {
				t.Fatalf("size = %q, want %q", got, tc.want)
			}
			wantExeCalls, wantStatCalls := 1, 1
			if tc.goos == "linux" {
				wantExeCalls = 0
			}
			if tc.exeErr != nil {
				wantStatCalls = 0
			}
			if exeCalls != wantExeCalls || statCalls != wantStatCalls {
				t.Fatalf("unexpected filesystem calls: executable=%d stat=%d", exeCalls, statCalls)
			}
		})
	}
}
