package server

import (
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	dbapi "github.com/Mar1eena/trb_proto/gen/go/api/db_api"
	testpb "github.com/Mar1eena/trb_proto/gen/go/api/test"
	tinvest "github.com/Mar1eena/trb_proto/gen/go/tinvest"
	"google.golang.org/grpc"
)

type Server struct {
	testpb.UnimplementedTestServer
	invest tinvest.InstrumentsServiceClient
	data   dbapi.DbApiClient
	log    zlog.Logger
}

var _ testpb.TestServer = (*Server)(nil)

func New(invest tinvest.InstrumentsServiceClient, data dbapi.DbApiClient, log zlog.Logger) *Server {
	return &Server{invest: invest, data: data, log: log}
}

func Register(srv *grpc.Server, service *Server) {
	testpb.RegisterTestServer(srv, service)
}
