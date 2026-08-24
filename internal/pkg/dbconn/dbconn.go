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

func LooksLikeAddr(value string) bool {
	_, _, ok := ParseHostPort(value)
	return ok
}
