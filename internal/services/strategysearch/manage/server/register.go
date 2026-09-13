package server

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	trb_nats "github.com/Mar1eena/TrB_V3/internal/pkg/brokers/nats"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	historiccandlepb "github.com/Mar1eena/trb_proto/gen/go/historiccandle"
	strategysearchpb "github.com/Mar1eena/trb_proto/gen/go/strategysearch"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

// Server реализует trb.strategysearch.v1.StrategySearchService.
type Server struct {
	strategysearchpb.UnimplementedStrategySearchServiceServer
	pg *pgxpool.Pool
	// ch — чтение равнины капитала/сделок/рядов индикаторов отдельных прогонов
	// бэктеста (GetBacktestResult). Таблицы (TrB_strategysearch.equity_curve/
	// trades/indicator_series) самопровизионирует strategysearch-engine (Python),
	// как и trials/studies для поиска — здесь только читаем.
	ch  driver.Conn
	js  *trb_nats.Nats
	log zlog.Logger
	// historicCandle — резолв MarketSpace в market_candidates на SubmitSearch
	// (ListLastDownloads: какие uid/interval реально есть в ClickHouse и за
	// какой период). nil => market_space в запросах отклоняется как недоступный.
	historicCandle historiccandlepb.HistoricCandleClient
}

func New(pg *pgxpool.Pool, ch driver.Conn, js *trb_nats.Nats, log zlog.Logger, historicCandle historiccandlepb.HistoricCandleClient) *Server {
	return &Server{pg: pg, ch: ch, js: js, log: log, historicCandle: historicCandle}
}

func Register(gs *grpc.Server, s *Server) {
	strategysearchpb.RegisterStrategySearchServiceServer(gs, s)
}
