package internal

import "os"

func GetAppSetting(setting string, defaultValue string) string {
	if appSettingValue, exists := os.LookupEnv(setting); exists {
		return appSettingValue
	}

	return defaultValue
}
