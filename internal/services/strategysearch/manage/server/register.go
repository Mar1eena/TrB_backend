package server

import (
	trb_nats "github.com/Mar1eena/TrB_V3/internal/pkg/brokers/nats"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	strategysearchpb "github.com/Mar1eena/trb_proto/gen/go/strategysearch"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

// Server реализует trb.strategysearch.v1.StrategySearchService.
type Server struct {
	strategysearchpb.UnimplementedStrategySearchServiceServer
	pg  *pgxpool.Pool
	js  *trb_nats.Nats
	log zlog.Logger
}

func New(pg *pgxpool.Pool, js *trb_nats.Nats, log zlog.Logger) *Server {
	return &Server{pg: pg, js: js, log: log}
}

func Register(gs *grpc.Server, s *Server) {
	strategysearchpb.RegisterStrategySearchServiceServer(gs, s)
}
