package server

import (
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	tinvest "github.com/Mar1eena/trb_proto/gen/go/api/tinvest"
	chpb "github.com/Mar1eena/trb_proto/gen/go/clickhouse"
	testpb "github.com/Mar1eena/trb_proto/gen/go/test"
	"google.golang.org/grpc"
)

type Server struct {
	testpb.UnimplementedTestServer
	invest tinvest.InstrumentsServiceClient
	ch     chpb.ClickHouseClient
	log    zlog.Logger
}

var _ testpb.TestServer = (*Server)(nil)

func New(invest tinvest.InstrumentsServiceClient, ch chpb.ClickHouseClient, log zlog.Logger) *Server {
	return &Server{invest: invest, ch: ch, log: log}
}

func Register(srv *grpc.Server, service *Server) {
	testpb.RegisterTestServer(srv, service)
}
