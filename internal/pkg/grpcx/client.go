package grpcx

import (
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc"
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
