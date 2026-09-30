package worker

import (
	"io/fs"
	"os"
	"runtime"
	"strconv"
)

func appBinarySize() string {
	return appBinarySizeWith(runtime.GOOS, os.Executable, os.Stat)
}

func appBinarySizeWith(goos string, executable func() (string, error), stat func(string) (fs.FileInfo, error)) string {
	// On Linux the procfs reference identifies the executing file even if its
	// pathname has been replaced or unlinked. Resolving it first loses that
	// identity; os.Executable also removes the kernel's " (deleted)" suffix.
	path := "/proc/self/exe"
	if goos != "linux" {
		var err error
		path, err = executable()
		if err != nil {
			return ""
		}
	}
	info, err := stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() < 0 {
		return ""
	}
	// Only return a numeric size. Paths and filesystem errors must not become
	// metadata, and failed collection must never fail worker initialization.
	return strconv.FormatInt(info.Size(), 10)
}
