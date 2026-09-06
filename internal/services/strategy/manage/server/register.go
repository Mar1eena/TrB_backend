package server

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	trb_nats "github.com/Mar1eena/TrB_V3/internal/pkg/brokers/nats"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	strategypb "github.com/Mar1eena/trb_proto/gen/go/strategy"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

// Server реализует trb.strategy.v1.StrategyService.
type Server struct {
	strategypb.UnimplementedStrategyServiceServer
	pg  *pgxpool.Pool
	ch  driver.Conn
	js  *trb_nats.Nats
	log zlog.Logger
}

func New(pg *pgxpool.Pool, ch driver.Conn, js *trb_nats.Nats, log zlog.Logger) *Server {
	return &Server{pg: pg, ch: ch, js: js, log: log}
}

func Register(gs *grpc.Server, s *Server) {
	strategypb.RegisterStrategyServiceServer(gs, s)
}
