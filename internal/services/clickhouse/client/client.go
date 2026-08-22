package client

import (
	"errors"
	"net"

	"github.com/Mar1eena/TrB_V3/internal/pkg/db/clickhouse"
	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
	"github.com/Mar1eena/TrB_V3/internal/pkg/grpcx"
	pb "github.com/Mar1eena/trb_proto/gen/go/clickhouse"
	"google.golang.org/grpc"
)

// Manager — gRPC-клиент ClickHouse_Admin (trb.clickhouse.v1).
type Manager struct {
	pb.ClickHouse_AdminClient
	conn *grpc.ClientConn
}

// Dial opens an insecure gRPC connection to the ClickHouse API service.
func Dial(addr string) (*grpc.ClientConn, error) {
	return grpcx.DialInsecure(addr)
}

// DialFromEnv dials using CLICKHOUSE_API_URL / CLICKHOUSE_API_URL_DOCKER.
func DialFromEnv() (*grpc.ClientConn, error) {
	addr := env.Addr("CLICKHOUSE_API_URL", "CLICKHOUSE_API_URL_DOCKER")
	if addr == "" {
		return nil, errors.New("CLICKHOUSE_API_URL не задан")
	}
	return Dial(addr)
}

// New returns a typed ClickHouse (business) client.
func New(conn grpc.ClientConnInterface) pb.ClickHouseClient {
	return pb.NewClickHouseClient(conn)
}

// NewAdmin returns a typed ClickHouse_Admin client.
func NewAdmin(conn grpc.ClientConnInterface) pb.ClickHouse_AdminClient {
	return pb.NewClickHouse_AdminClient(conn)
}

// NewManager dials ClickHouse_Admin using Host/GrpcPort from conf.
func NewManager(conf clickhouse.ClickHouseConfig) (*Manager, error) {
	addr := net.JoinHostPort(conf.Host, conf.GrpcPort)
	conn, err := Dial(addr)
	if err != nil {
		return nil, err
	}
	return &Manager{
		ClickHouse_AdminClient: NewAdmin(conn),
		conn:                   conn,
	}, nil
}

func (m *Manager) Close() error {
	if m == nil || m.conn == nil {
		return nil
	}
	return m.conn.Close()
}
