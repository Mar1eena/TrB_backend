package client

import (
	"net"

	"github.com/Mar1eena/TrB_V3/internal/pkg/db/clickhouse"
	"github.com/Mar1eena/TrB_V3/internal/pkg/grpcx"
	pb "github.com/Mar1eena/trb_proto/gen/go/api/clickhouse"
	"google.golang.org/grpc"
)

// Manager — gRPC-клиент ClickHouseManager
// (trb.clickhouse.manager.public.contract.v1).
type Manager struct {
	pb.ClickHouseManagerClient
	conn *grpc.ClientConn
}

// Dial opens an insecure gRPC connection to the ClickHouse manager service.
func Dial(addr string) (*grpc.ClientConn, error) {
	return grpcx.DialInsecure(addr)
}

// New returns a typed ClickHouseManager client.
func New(conn grpc.ClientConnInterface) pb.ClickHouseManagerClient {
	return pb.NewClickHouseManagerClient(conn)
}

// NewManager dials ClickHouse manager using Host/GrpcPort from conf.
func NewManager(conf clickhouse.ClickHouseConfig) (*Manager, error) {
	addr := net.JoinHostPort(conf.Host, conf.GrpcPort)
	conn, err := Dial(addr)
	if err != nil {
		return nil, err
	}
	return &Manager{
		ClickHouseManagerClient: New(conn),
		conn:                    conn,
	}, nil
}

func (m *Manager) Close() error {
	if m == nil || m.conn == nil {
		return nil
	}
	return m.conn.Close()
}
