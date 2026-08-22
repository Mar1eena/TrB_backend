package grpcx

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestSplitFullMethod(t *testing.T) {
	svc, method := splitFullMethod("/trb.clickhouse.manager.public.contract.v1.ClickHouseManager/Query")
	if svc != "trb.clickhouse.manager.public.contract.v1.ClickHouseManager" {
		t.Fatalf("service: %q", svc)
	}
	if method != "Query" {
		t.Fatalf("method: %q", method)
	}
}

func TestSkipHealth(t *testing.T) {
	if !skipMethod("/grpc.health.v1.Health/Check") {
		t.Fatal("health не должен попадать в access-лог")
	}
	if skipMethod("/trb.db.api.public.contract.v1.DbApi/ListInstruments") {
		t.Fatal("обычный метод не скип")
	}
}

const bufSize = 1024 * 1024

type testServer struct {
	grpc.ServerStream
	receivedTrace TraceContext
}

func (s *testServer) UnaryCall(ctx context.Context, req any) (any, error) {
	tc, ok := TraceFromContext(ctx)
	if ok {
		s.receivedTrace = tc
	}
	return &emptypb.Empty{}, nil
}

func TestEndToEnd_UnaryTracePropagation(t *testing.T) {
	var buf bytes.Buffer
	z := zerolog.New(&buf)
	logger := zlog.Logger{Logger: z}

	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer(ServerOptions(logger)...)

	// Регистрируем mock handler через generic interceptor/handler
	ts := &testServer{}
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.TestService",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Ping",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					in := new(emptypb.Empty)
					if err := dec(in); err != nil {
						return nil, err
					}
					if interceptor == nil {
						return ts.UnaryCall(ctx, in)
					}
					info := &grpc.UnaryServerInfo{
						Server:     srv,
						FullMethod: "/test.TestService/Ping",
					}
					handler := func(ctx context.Context, req any) (any, error) {
						return ts.UnaryCall(ctx, req)
					}
					return interceptor(ctx, in, info, handler)
				},
			},
		},
		Streams: []grpc.StreamDesc{},
	}, ts)

	go func() {
		_ = s.Serve(lis)
	}()
	defer s.Stop()

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(UnaryClientTrace(logger)),
		grpc.WithChainStreamInterceptor(StreamClientTrace(logger)),
	)
	if err != nil {
		t.Fatalf("failed to dial bufnet: %v", err)
	}
	defer conn.Close()

	// 1. Клиент делает запрос с заданным TraceContext
	initialTC := NewTraceContext()
	ctx := ContextWithTrace(context.Background(), initialTC)

	var reply emptypb.Empty
	err = conn.Invoke(ctx, "/test.TestService/Ping", &emptypb.Empty{}, &reply)
	if err != nil {
		t.Fatalf("invoke failed: %v", err)
	}

	// 2. Проверяем, что сервер получил тот же TraceID
	if ts.receivedTrace.TraceID != initialTC.TraceID {
		t.Fatalf("expected server TraceID %s == client TraceID %s", ts.receivedTrace.TraceID, initialTC.TraceID)
	}
	if ts.receivedTrace.SpanID == "" {
		t.Fatal("expected non-empty SpanID on server")
	}

	// 3. Проверяем логи
	logOutput := buf.String()
	if !strings.Contains(logOutput, initialTC.TraceID) {
		t.Fatalf("log output should contain trace_id %s, got:\n%s", initialTC.TraceID, logOutput)
	}
	if !strings.Contains(logOutput, "gRPC request") {
		t.Fatalf("log output should contain 'gRPC request', got:\n%s", logOutput)
	}
	if !strings.Contains(logOutput, "gRPC client request") {
		t.Fatalf("log output should contain 'gRPC client request', got:\n%s", logOutput)
	}
}

func TestEndToEnd_MultiHopDistributedTracing(t *testing.T) {
	logger := zlog.Logger{Logger: zerolog.Nop()}

	// Сервис C (конечный сервис)
	lisC := bufconn.Listen(bufSize)
	sC := grpc.NewServer(ServerOptions(logger)...)
	tsC := &testServer{}
	sC.RegisterService(&grpc.ServiceDesc{
		ServiceName: "service.C",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "DoWork",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					in := new(emptypb.Empty)
					if err := dec(in); err != nil {
						return nil, err
					}
					if interceptor == nil {
						return tsC.UnaryCall(ctx, in)
					}
					info := &grpc.UnaryServerInfo{
						Server:     srv,
						FullMethod: "/service.C/DoWork",
					}
					return interceptor(ctx, in, info, func(ctx context.Context, req any) (any, error) {
						return tsC.UnaryCall(ctx, req)
					})
				},
			},
		},
		Streams: []grpc.StreamDesc{},
	}, tsC)
	go func() { _ = sC.Serve(lisC) }()
	defer sC.Stop()

	connC, err := grpc.NewClient(
		"passthrough://bufnet-c",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lisC.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(UnaryClientTrace(logger)),
	)
	if err != nil {
		t.Fatalf("failed to dial C: %v", err)
	}
	defer connC.Close()

	// Сервис B (промежуточный сервис, вызывает C)
	lisB := bufconn.Listen(bufSize)
	sB := grpc.NewServer(ServerOptions(logger)...)
	tsB := &testServer{}
	sB.RegisterService(&grpc.ServiceDesc{
		ServiceName: "service.B",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Forward",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					in := new(emptypb.Empty)
					if err := dec(in); err != nil {
						return nil, err
					}
					info := &grpc.UnaryServerInfo{
						Server:     srv,
						FullMethod: "/service.B/Forward",
					}
					return interceptor(ctx, in, info, func(ctx context.Context, req any) (any, error) {
						tsB.UnaryCall(ctx, req)
						// Вызываем сервис C, передавая входящий ctx
						var reply emptypb.Empty
						return &reply, connC.Invoke(ctx, "/service.C/DoWork", &emptypb.Empty{}, &reply)
					})
				},
			},
		},
		Streams: []grpc.StreamDesc{},
	}, tsB)
	go func() { _ = sB.Serve(lisB) }()
	defer sB.Stop()

	connB, err := grpc.NewClient(
		"passthrough://bufnet-b",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lisB.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(UnaryClientTrace(logger)),
	)
	if err != nil {
		t.Fatalf("failed to dial B: %v", err)
	}
	defer connB.Close()

	// Инициатор A вызывает B
	rootTC := NewTraceContext()
	ctxA := ContextWithTrace(context.Background(), rootTC)

	var reply emptypb.Empty
	err = connB.Invoke(ctxA, "/service.B/Forward", &emptypb.Empty{}, &reply)
	if err != nil {
		t.Fatalf("invoke B failed: %v", err)
	}

	// Проверяем:
	// 1. TraceID везде одинаковый!
	if tsB.receivedTrace.TraceID != rootTC.TraceID {
		t.Fatalf("service B TraceID %s != root TraceID %s", tsB.receivedTrace.TraceID, rootTC.TraceID)
	}
	if tsC.receivedTrace.TraceID != rootTC.TraceID {
		t.Fatalf("service C TraceID %s != root TraceID %s", tsC.receivedTrace.TraceID, rootTC.TraceID)
	}

	// 2. ParentSpanID у сервиса C ссылается на span, созданный при вызове из сервиса B
	if tsC.receivedTrace.ParentSpanID == "" {
		t.Fatal("expected non-empty ParentSpanID on service C")
	}
	if tsC.receivedTrace.SpanID == tsB.receivedTrace.SpanID {
		t.Fatal("service C and service B should have distinct SpanIDs")
	}
}

func TestWithTraceLogger(t *testing.T) {
	var buf bytes.Buffer
	z := zerolog.New(&buf)
	logger := zlog.Logger{Logger: z}

	tc := TraceContext{
		TraceID:      "11112222333344445555666677778888",
		SpanID:       "aaaabbbbccccdddd",
		ParentSpanID: "0000111122223333",
		Sampled:      true,
	}
	ctx := ContextWithTrace(context.Background(), tc)

	tracedLogger := WithTrace(logger, ctx)
	tracedLogger.Info().Msg("custom event with trace")

	out := buf.String()
	if !strings.Contains(out, "11112222333344445555666677778888") {
		t.Fatalf("expected trace_id in log, got: %s", out)
	}
	if !strings.Contains(out, "aaaabbbbccccdddd") {
		t.Fatalf("expected span_id in log, got: %s", out)
	}
	if !strings.Contains(out, "0000111122223333") {
		t.Fatalf("expected parent_span_id in log, got: %s", out)
	}
}

func TestLogClientRPC_ErrorLevels(t *testing.T) {
	var buf bytes.Buffer
	z := zerolog.New(&buf)
	logger := zlog.Logger{Logger: z}

	tc := NewTraceContext()

	// 1. Remote server error (Internal) should be logged as warn to avoid cascading errors
	logClientRPC(context.Background(), logger, tc, "/service.A/Method", "unary", "target:9091", time.Now(), status.Error(codes.Internal, "remote internal error"))
	out := buf.String()
	if !strings.Contains(out, `"level":"warn"`) {
		t.Fatalf("expected level:warn for remote Internal error, got: %s", out)
	}

	buf.Reset()

	// 2. Transport unavailable error should be logged as error
	logClientRPC(context.Background(), logger, tc, "/service.A/Method", "unary", "target:9091", time.Now(), status.Error(codes.Unavailable, "transport unavailable"))
	out2 := buf.String()
	if !strings.Contains(out2, `"level":"error"`) {
		t.Fatalf("expected level:error for Unavailable error, got: %s", out2)
	}
}

// Заглушка для io.Closer
var _ io.Closer = (*bufconn.Listener)(nil)
