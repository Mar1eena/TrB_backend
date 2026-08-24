package dbconn

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"
)

// MetadataKey is the gRPC/HTTP header the UI sends to pick a named DB target.
const MetadataKey = "x-trb-connection"

func FromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	values := md.Get(MetadataKey)
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}

// LooksLikeAddr reports whether the UI sent a host:port instead of a named extra.
func LooksLikeAddr(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || strings.ContainsAny(value, " /\\") {
		return false
	}
	host, port, ok := strings.Cut(value, ":")
	if !ok || host == "" || port == "" {
		return false
	}
	for _, r := range port {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
