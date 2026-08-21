// Package grpcx — общие опции gRPC-серверов.
package grpcx

import (
	"context"
	"strings"
	"time"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// ServerOptions включает access-лог: поля уходят в JSON и дальше в HyperDX
// как LogAttributes (не в body).
func ServerOptions(l zlog.Logger) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(UnaryAccessLog(l)),
		grpc.ChainStreamInterceptor(StreamAccessLog(l)),
		grpc.MaxRecvMsgSize(MaxMsgSize),
		grpc.MaxSendMsgSize(MaxMsgSize),
	}
}

func UnaryAccessLog(l zlog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if skipMethod(info.FullMethod) {
			return handler(ctx, req)
		}
		start := time.Now()
		resp, err := handler(ctx, req)
		logRPC(ctx, l, info.FullMethod, "unary", start, err)
		return resp, err
	}
}

func StreamAccessLog(l zlog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if skipMethod(info.FullMethod) {
			return handler(srv, ss)
		}
		start := time.Now()
		err := handler(srv, ss)
		logRPC(ss.Context(), l, info.FullMethod, "stream", start, err)
		return err
	}
}

func skipMethod(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/grpc.health.v1.Health/") ||
		strings.HasPrefix(fullMethod, "/grpc.reflection.v1")
}

func splitFullMethod(fullMethod string) (service, method string) {
	s := strings.TrimPrefix(fullMethod, "/")
	service, method, ok := strings.Cut(s, "/")
	if !ok {
		return s, ""
	}
	return service, method
}

func firstMD(md metadata.MD, key string) string {
	vals := md.Get(key)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

func logRPC(ctx context.Context, l zlog.Logger, fullMethod, rpcKind string, start time.Time, err error) {
	service, method := splitFullMethod(fullMethod)
	code := codes.OK
	if err != nil {
		code = status.Code(err)
	}

	ev := l.Info()
	switch code {
	case codes.OK, codes.Canceled, codes.DeadlineExceeded:
		if code != codes.OK {
			ev = l.Warn()
		}
	default:
		ev = l.Error().Err(err)
	}

	peerAddr := ""
	if p, ok := peer.FromContext(ctx); ok && p != nil && p.Addr != nil {
		peerAddr = p.Addr.String()
	}
	md, _ := metadata.FromIncomingContext(ctx)
	clientAddr := firstMD(md, "x-forwarded-for")
	if clientAddr == "" {
		clientAddr = firstMD(md, "x-real-ip")
	}

	ev = ev.
		Str("rpc.system", "grpc").
		Str("rpc.kind", rpcKind).
		Str("grpc.service", service).
		Str("grpc.method", method).
		Str("grpc.code", code.String()).
		Int64("duration_ms", time.Since(start).Milliseconds()).
		Str("peer.addr", peerAddr)
	if clientAddr != "" {
		ev = ev.Str("client.addr", clientAddr)
	}
	if ua := firstMD(md, "user-agent"); ua != "" {
		ev = ev.Str("user_agent", ua)
	}
	ev.Msg("gRPC request")
}
