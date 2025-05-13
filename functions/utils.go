package functions

import (
	"os"
	"strings"
)

func GetAppSetting(setting string, defaultValue string) string {
	if appSettingValue, exists := os.LookupEnv(setting); exists {
		return appSettingValue
	}

	return defaultValue
}

// Host sends functions uri with http:// prefix and trailing /
// gRPC does not accept these addresses
func CleanAddressForGrpc(uri string) string {
	addr := strings.TrimPrefix(uri, "http://")
	addr = strings.TrimSuffix(addr, "/")

	return addr
}
