package server

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	dbapi "github.com/Mar1eena/trb_proto/gen/go/api/db_api"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

type Server struct {
	dbapi.UnimplementedDbApiServer
	ch  driver.Conn
	pg  *pgxpool.Pool
	log zlog.Logger
}

var _ dbapi.DbApiServer = (*Server)(nil)

func New(ch driver.Conn, pg *pgxpool.Pool, log zlog.Logger) *Server {
	return &Server{ch: ch, pg: pg, log: log}
}

func Register(srv *grpc.Server, service *Server) {
	dbapi.RegisterDbApiServer(srv, service)
}
