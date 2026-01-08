package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// GetEnv returns env value or default if empty.
func GetEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// GetEnvInt returns env value parsed as int or fallback if empty/invalid.
func GetEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// GetEnvDuration parses duration or returns fallback if empty/invalid.
func GetEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

// LoadEnv loads .env if present; missing file is ignored.
func LoadEnv() {
	_ = godotenv.Load()
}
