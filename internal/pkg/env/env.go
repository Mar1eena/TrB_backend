package env

import (
	"os"
	"strings"

	"github.com/joho/godotenv"
)

// Load reads .env files if they exist. Missing files are OK — in Docker/K8s
// variables are usually injected by the runtime. Existing process env always
// wins over values from files (godotenv default).
func Load(paths ...string) error {
	if len(paths) == 0 {
		paths = []string{".env", "/app/.env"}
	}

	var loaded bool
	for _, path := range paths {
		err := godotenv.Load(path)
		if err == nil {
			loaded = true
			continue
		}
		if os.IsNotExist(err) {
			continue
		}
		// godotenv may wrap the not-exist error as a plain message
		if isNotExistMessage(err) {
			continue
		}
		return err
	}
	_ = loaded
	return nil
}

func isNotExistMessage(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "no such file") || strings.Contains(msg, "cannot find the file")
}

// IsContainer reports whether the process runs inside Docker/container.
// Prefer APP_RUNTIME=docker (set in the service image); /.dockerenv is a
// fallback for non-scratch images.
func IsContainer() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("APP_RUNTIME"))) {
	case "docker", "container":
		return true
	}
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return true
	}
	return false
}

// Addr returns the docker-specific URL inside a container, otherwise the local one.
func Addr(localKey, dockerKey string) string {
	if IsContainer() {
		if v := strings.TrimSpace(os.Getenv(dockerKey)); v != "" {
			return v
		}
	}
	return strings.TrimSpace(os.Getenv(localKey))
}

// Get returns a trimmed environment value.
func Get(key string) string {
	return strings.TrimSpace(os.Getenv(key))
}

// First returns the first non-empty value among keys.
func First(keys ...string) string {
	for _, key := range keys {
		if v := Get(key); v != "" {
			return v
		}
	}
	return ""
}
