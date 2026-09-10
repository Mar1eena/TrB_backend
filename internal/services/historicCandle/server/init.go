package server

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	hcpb "github.com/Mar1eena/trb_proto/gen/go/historiccandle"
	"google.golang.org/grpc"
)

// Server реализует trb.historiccandle.v1.HistoricCandle: чтение исторических
// свечей (TrB.hct) и журнала загрузок (TrB.hct_last_download) из ClickHouse.
type Server struct {
	hcpb.UnimplementedHistoricCandleServer
	ch  driver.Conn
	log zlog.Logger
}

var _ hcpb.HistoricCandleServer = (*Server)(nil)

func New(ch driver.Conn, log zlog.Logger) *Server {
	return &Server{ch: ch, log: log}
}

func Register(srv *grpc.Server, service *Server) {
	hcpb.RegisterHistoricCandleServer(srv, service)
}
