package functions

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

// Host sends functions uri with http:// prefix and trailing /
// gRPC does not accept these addresses
func CleanAddressForGrpc(uri string) string {
	addr := strings.TrimPrefix(uri, "http://")
	addr = strings.TrimSuffix(addr, "/")

	return addr
}

func GetFunctionName(f interface{}) string {
	strs := strings.Split((runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()), ".")
	return strs[len(strs)-1]
}

func getStableHash(s string) uint32 {
	var hash uint32 = 23
	for _, c := range s {
		hash = hash*31 + uint32(c)
	}

	return hash
}

// returns a stable string hash ID for a function
func HashFunctionID(f RegisteredFunction) (string, error) {
	var hash uint32 = 17
	atLeastOnePresent := false

	if f.FuncName != "" {
		atLeastOnePresent = true
		hash = hash*31 + getStableHash(f.FuncName)
	}

	if len(f.RawBindings) > 0 {
		for _, binding := range f.RawBindings {
			hash = hash*31 + getStableHash(fmt.Sprintf("%v", binding))
		}
	}

	if atLeastOnePresent {
		id := uintToString(hash)
		return id, nil
	}

	return "", fmt.Errorf("failed to generate function ID for function %s", f.FuncName)
}

func uintToString(u uint32) string {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, u)
	return string([]byte{
		'0' + byte((u/100000000)%10),
		'0' + byte((u/10000000)%10),
		'0' + byte((u/1000000)%10),
		'0' + byte((u/100000)%10),
		'0' + byte((u/10000)%10),
		'0' + byte((u/1000)%10),
		'0' + byte((u/100)%10),
		'0' + byte((u/10)%10),
		'0' + byte(u%10),
	})
}
