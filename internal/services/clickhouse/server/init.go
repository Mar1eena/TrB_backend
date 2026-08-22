package server

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	chmgr "github.com/Mar1eena/trb_proto/gen/go/clickhouse"
	"google.golang.org/grpc"
)

type Server struct {
	chmgr.UnimplementedClickHouse_AdminServer
	chmgr.UnimplementedClickHouseServer
	ch  driver.Conn
	log zlog.Logger
}

var (
	_ chmgr.ClickHouse_AdminServer = (*Server)(nil)
	_ chmgr.ClickHouseServer       = (*Server)(nil)
)

func New(ch driver.Conn, log zlog.Logger) *Server {
	return &Server{ch: ch, log: log}
}

func Register(srv *grpc.Server, service *Server) {
	chmgr.RegisterClickHouse_AdminServer(srv, service)
	chmgr.RegisterClickHouseServer(srv, service)
}
