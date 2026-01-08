package config

import "github.com/joho/godotenv"

// LoadEnv loads .env if present; missing file is ignored.
func LoadEnv() {
	_ = godotenv.Load()
}
