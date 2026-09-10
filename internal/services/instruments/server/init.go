package server

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	tinvest "github.com/Mar1eena/trb_proto/gen/go/api/tinvest"
	instrpb "github.com/Mar1eena/trb_proto/gen/go/instruments"
	"google.golang.org/grpc"
)

// Server реализует trb.instruments.v1.Instruments: каталог инструментов в
// ClickHouse (TrB.sht) и синхронизацию справочника из invest.
type Server struct {
	instrpb.UnimplementedInstrumentsServer
	ch     driver.Conn
	invest tinvest.InstrumentsServiceClient
	log    zlog.Logger
}

var _ instrpb.InstrumentsServer = (*Server)(nil)

func New(ch driver.Conn, invest tinvest.InstrumentsServiceClient, log zlog.Logger) *Server {
	return &Server{ch: ch, invest: invest, log: log}
}

func Register(srv *grpc.Server, service *Server) {
	instrpb.RegisterInstrumentsServer(srv, service)
}
