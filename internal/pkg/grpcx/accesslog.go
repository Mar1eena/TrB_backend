// Package grpcx — общие опции, интерцепторы, логирование и распределённая трассировка gRPC.
package grpcx

import (
	"context"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// ServerOptions возвращает базовый набор опций для gRPC-сервера,
// включая перехватчики входящих запросов с извлечением TraceContext,
// логированием метаданных и лимитами на размер сообщений.
func ServerOptions(l zlog.Logger) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(UnaryAccessLog(l)),
		grpc.ChainStreamInterceptor(StreamAccessLog(l)),
		grpc.MaxRecvMsgSize(MaxMsgSize),
		grpc.MaxSendMsgSize(MaxMsgSize),
	}
}

// ClientOptions возвращает базовые опции gRPC-клиента с интерцепторами трассировки и логирования.
func ClientOptions(l zlog.Logger) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(UnaryClientTrace(l)),
		grpc.WithChainStreamInterceptor(StreamClientTrace(l)),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxMsgSize),
			grpc.MaxCallSendMsgSize(MaxMsgSize),
		),
	}
}

// UnaryAccessLog перехватывает входящие унарные вызовы, извлекает или генерирует
// TraceContext, сохраняет его в контекст запроса и логирует результат.
func UnaryAccessLog(l zlog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if skipMethod(info.FullMethod) {
			return handler(ctx, req)
		}
		ctx, tc := ExtractOrGenerateTrace(ctx)
		start := time.Now()
		resp, err := handler(ctx, req)
		logServerRPC(ctx, l, tc, info.FullMethod, "unary", start, err)
		return resp, err
	}
}

type serverStreamWithContext struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *serverStreamWithContext) Context() context.Context {
	return s.ctx
}

// StreamAccessLog перехватывает входящие потоковые вызовы, обогащает контекст
// потока данными трассировки и логирует завершение стрима.
func StreamAccessLog(l zlog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if skipMethod(info.FullMethod) {
			return handler(srv, ss)
		}
		ctx, tc := ExtractOrGenerateTrace(ss.Context())
		wrappedSS := &serverStreamWithContext{
			ServerStream: ss,
			ctx:          ctx,
		}
		start := time.Now()
		err := handler(srv, wrappedSS)
		logServerRPC(ctx, l, tc, info.FullMethod, "stream", start, err)
		return err
	}
}

// UnaryClientTrace перехватывает исходящие унарные вызовы между сервисами,
// создаёт дочерний спан, пробрасывает заголовки W3C traceparent и логирует запрос.
func UnaryClientTrace(l zlog.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if skipMethod(method) {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		tc, ok := TraceFromContext(ctx)
		if !ok {
			tc = NewTraceContext()
		}
		childTC := tc.NewChildSpan()
		ctx = ContextWithTrace(ctx, childTC)
		ctx = InjectOutgoingMetadata(ctx, childTC)

		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		logClientRPC(ctx, l, childTC, method, "unary", cc.Target(), start, err)
		return err
	}
}

// StreamClientTrace перехватывает исходящие потоковые вызовы между сервисами,
// пробрасывает контекст трассировки в исходящие метаданные и логирует стрим.
func StreamClientTrace(l zlog.Logger) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if skipMethod(method) {
			return streamer(ctx, desc, cc, method, opts...)
		}
		tc, ok := TraceFromContext(ctx)
		if !ok {
			tc = NewTraceContext()
		}
		childTC := tc.NewChildSpan()
		ctx = ContextWithTrace(ctx, childTC)
		ctx = InjectOutgoingMetadata(ctx, childTC)

		start := time.Now()
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			logClientRPC(ctx, l, childTC, method, "stream", cc.Target(), start, err)
			return nil, err
		}
		return &tracedClientStream{
			ClientStream: cs,
			ctx:          ctx,
			l:            l,
			tc:           childTC,
			method:       method,
			target:       cc.Target(),
			start:        start,
		}, nil
	}
}

type tracedClientStream struct {
	grpc.ClientStream
	ctx    context.Context
	l      zlog.Logger
	tc     TraceContext
	method string
	target string
	start  time.Time
	once   sync.Once
}

func (s *tracedClientStream) Context() context.Context {
	return s.ctx
}

func (s *tracedClientStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil {
		s.once.Do(func() {
			logErr := err
			if logErr == io.EOF {
				logErr = nil
			}
			logClientRPC(s.ctx, s.l, s.tc, s.method, "stream", s.target, s.start, logErr)
		})
	}
	return err
}

func (s *tracedClientStream) CloseSend() error {
	err := s.ClientStream.CloseSend()
	if err != nil {
		s.once.Do(func() {
			logClientRPC(s.ctx, s.l, s.tc, s.method, "stream", s.target, s.start, err)
		})
	}
	return err
}

// WithTrace возвращает логгер, обогащённый полями trace_id, span_id и parent_span_id из context.Context.
func WithTrace(l zlog.Logger, ctx context.Context) zlog.Logger {
	if tc, ok := TraceFromContext(ctx); ok {
		ev := l.With().
			Str("trace_id", tc.TraceID).
			Str("span_id", tc.SpanID)
		if tc.ParentSpanID != "" {
			ev = ev.Str("parent_span_id", tc.ParentSpanID)
		}
		return zlog.Logger{Logger: ev.Logger()}
	}
	return l
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

func logServerRPC(ctx context.Context, l zlog.Logger, tc TraceContext, fullMethod, rpcKind string, start time.Time, err error) {
	service, method := splitFullMethod(fullMethod)
	code := codes.OK
	if err != nil {
		code = status.Code(err)
	}

	ev := l.Info()
	switch code {
	case codes.OK:
		ev = l.Info()
	case codes.Canceled, codes.DeadlineExceeded, codes.NotFound, codes.AlreadyExists, codes.InvalidArgument:
		ev = l.Warn()
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

	dur := time.Since(start)
	ev = ev.
		Str("trace_id", tc.TraceID).
		Str("span_id", tc.SpanID).
		Str("rpc.system", "grpc").
		Str("rpc.kind", "server").
		Str("rpc.type", rpcKind).
		Str("grpc.service", service).
		Str("grpc.method", method).
		Str("grpc.code", code.String()).
		Uint32("grpc.status_code", uint32(code)).
		Int64("duration_ms", dur.Milliseconds()).
		Int64("duration_us", dur.Microseconds()).
		Str("peer.addr", peerAddr)

	if tc.ParentSpanID != "" {
		ev = ev.Str("parent_span_id", tc.ParentSpanID)
	}
	if clientAddr != "" {
		ev = ev.Str("client.addr", clientAddr)
	}
	if ua := firstMD(md, "user-agent"); ua != "" {
		ev = ev.Str("user_agent", ua)
	}
	if reqID := firstMD(md, "x-request-id"); reqID != "" {
		ev = ev.Str("x_request_id", reqID)
	}
	if appName := firstMD(md, "x-app-name"); appName != "" {
		ev = ev.Str("x_app_name", appName)
	}
	if err != nil {
		ev = ev.Str("grpc.error_message", status.Convert(err).Message())
	}

	ev.Msg("gRPC request")
}

func logClientRPC(ctx context.Context, l zlog.Logger, tc TraceContext, fullMethod, rpcKind, target string, start time.Time, err error) {
	service, method := splitFullMethod(fullMethod)
	code := codes.OK
	if err != nil {
		code = status.Code(err)
	}

	ev := l.Info()
	switch code {
	case codes.OK:
		ev = l.Info()
	case codes.Canceled, codes.DeadlineExceeded, codes.NotFound, codes.AlreadyExists, codes.InvalidArgument:
		ev = l.Warn()
	default:
		ev = l.Error().Err(err)
	}

	dur := time.Since(start)
	ev = ev.
		Str("trace_id", tc.TraceID).
		Str("span_id", tc.SpanID).
		Str("rpc.system", "grpc").
		Str("rpc.kind", "client").
		Str("rpc.type", rpcKind).
		Str("grpc.service", service).
		Str("grpc.method", method).
		Str("grpc.code", code.String()).
		Uint32("grpc.status_code", uint32(code)).
		Int64("duration_ms", dur.Milliseconds()).
		Int64("duration_us", dur.Microseconds()).
		Str("peer.addr", target)

	if tc.ParentSpanID != "" {
		ev = ev.Str("parent_span_id", tc.ParentSpanID)
	}
	if err != nil {
		ev = ev.Str("grpc.error_message", status.Convert(err).Message())
	}

	ev.Msg("gRPC client request")
}
