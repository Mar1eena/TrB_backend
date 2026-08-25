package grpcx

import (
	"context"

	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// MaxMsgSize — лимит unary-сообщений (справочник акций ~тысячи инструментов).
const MaxMsgSize = 64 << 20

// DialInsecure открывает соединение к gRPC-сервису без TLS,
// автоматически подключая интерцепторы логирования и передачи контекста трассировки.
func DialInsecure(addr string, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return DialInsecureWithLogger(addr, zlog.New(), extraOpts...)
}

// DialInsecureWithLogger открывает соединение к gRPC-сервису с переданным логгером.
func DialInsecureWithLogger(addr string, l zlog.Logger, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(UnaryClientTrace(l)),
		grpc.WithChainStreamInterceptor(StreamClientTrace(l)),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxMsgSize),
			grpc.MaxCallSendMsgSize(MaxMsgSize),
		),
	}
	opts = append(opts, extraOpts...)
	return grpc.NewClient(addr, opts...)
}

// WaitReady blocks until conn reaches Ready or ctx is canceled.
func WaitReady(ctx context.Context, conn *grpc.ClientConn) error {
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if !conn.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
}

// DialInsecureReady dials and waits until the server accepts connections.
func DialInsecureReady(ctx context.Context, addr string, l zlog.Logger, extraOpts ...grpc.DialOption) (*grpc.ClientConn, error) {
	conn, err := DialInsecureWithLogger(addr, l, extraOpts...)
	if err != nil {
		return nil, err
	}
	if err := WaitReady(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}
