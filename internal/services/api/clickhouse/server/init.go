package server

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	chmgr "github.com/Mar1eena/trb_proto/gen/go/api/clickhouse"
	"google.golang.org/grpc"
)

type Server struct {
	chmgr.UnimplementedClickHouseManagerServer
	ch  driver.Conn
	log zlog.Logger
}

var _ chmgr.ClickHouseManagerServer = (*Server)(nil)

func New(ch driver.Conn, log zlog.Logger) *Server {
	return &Server{ch: ch, log: log}
}

func Register(srv *grpc.Server, service *Server) {
	chmgr.RegisterClickHouseManagerServer(srv, service)
}
