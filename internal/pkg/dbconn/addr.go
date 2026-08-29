package dbconn

import (
	"net"
	"net/url"
	"strings"

	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
)

// ParseHostPort extracts host and port from host:port or http(s)://host:port/.
func ParseHostPort(value string) (host, port string, ok bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", "", false
	}
	if strings.Contains(value, "://") {
		u, err := url.Parse(value)
		if err != nil || u.Host == "" {
			return "", "", false
		}
		value = u.Host
	}
	value = strings.TrimSuffix(value, "/")
	h, p, err := net.SplitHostPort(value)
	if err != nil {
		return "", "", false
	}
	h = strings.TrimSpace(h)
	p = strings.TrimSpace(p)
	if h == "" || p == "" {
		return "", "", false
	}
	for _, r := range p {
		if r < '0' || r > '9' {
			return "", "", false
		}
	}
	return h, p, true
}

func loopbackHost(host string) bool {
	h := strings.Trim(strings.ToLower(host), "[]")
	return h == "localhost" || h == "127.0.0.1" || h == "::1" || h == "0.0.0.0" || h == "host.docker.internal"
}

func joinHostPort(host, port string) string {
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		return net.JoinHostPort(host, port)
	}
	return net.JoinHostPort(host, port)
}

func dockerNative(host, port string) (string, string) {
	switch port {
	case "8123":
		port = "9000"
	case "8124":
		port = "9000"
	}
	if !loopbackHost(host) {
		return host, port
	}
	switch port {
	case "9000":
		return "clickhouse-db", "9000"
	case "9001":
		return "clickhouse-db", "9000"
	case "5432":
		return "postgre-db", "5432"
	case "5435":
		return "postgre-1c-db", "5432"
	default:
		return "localhost", port
	}
}

func hostNative(host, port string) (string, string) {
	switch port {
	case "8123":
		return host, "9000"
	case "8124":
		return host, "9000"
	default:
		return host, port
	}
}

// Canonical maps a UI address onto the address this process should dial.
// Inside Docker, published localhost ports become compose service names.
func Canonical(value string) string {
	value = strings.TrimSpace(value)
	host, port, ok := ParseHostPort(value)
	if !ok {
		return value
	}
	if env.IsContainer() {
		host, port = dockerNative(host, port)
	} else {
		host, port = hostNative(host, port)
	}
	return joinHostPort(host, port)
}

// SameAddr reports whether two UI/dial strings refer to the same target.
func SameAddr(a, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	if a == "" || b == "" {
		return false
	}
	if strings.EqualFold(a, b) {
		return true
	}
	return Canonical(a) == Canonical(b)
}
