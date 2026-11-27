package utils

import (
	"os"
	"strconv"
	"strings"
	"time"
)

func Env(key, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}

func EnvBool(key string, fallback bool) bool {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(val)
	if err != nil {
		return fallback
	}
	return parsed
}

// EnvDuration parses a positive duration such as "10m" or "720h" from key. It
// returns fallback when the variable is unset or invalid.
func EnvDuration(key string, fallback time.Duration) time.Duration {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(val)
	if err != nil || parsed <= 0 {
		return fallback
	}

	return parsed
}
