package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	chpkg "github.com/Mar1eena/TrB_V3/internal/services/clickhouse/pkg"
	chmgr "github.com/Mar1eena/trb_proto/gen/go/clickhouse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	listCandlesDefaultLimit = 500
	listCandlesMaxLimit     = 10000
)

type historicCandleRow struct {
	Time         time.Time `ch:"time"`
	Open         float64   `ch:"open"`
	High         float64   `ch:"high"`
	Low          float64   `ch:"low"`
	Close        float64   `ch:"close"`
	Volume       int64     `ch:"volume"`
	VolumeBuy    int64     `ch:"volume_buy"`
	VolumeSell   int64     `ch:"volume_sell"`
	CandleSource int32     `ch:"candle_source"`
	IsComplete   bool      `ch:"is_complete"`
}

func (s *Server) ListCandles(ctx context.Context, req *chmgr.ListCandlesRequest) (*chmgr.ListCandlesResponse, error) {
	if req == nil {
		req = &chmgr.ListCandlesRequest{}
	}
	uid := strings.TrimSpace(req.GetUid())
	if uid == "" {
		return nil, status.Error(codes.InvalidArgument, "uid обязателен")
	}
	if req.GetInterval() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "interval обязателен")
	}
	if req.GetFrom() == nil || req.GetTo() == nil {
		return nil, status.Error(codes.InvalidArgument, "from и to обязательны")
	}
	from := req.GetFrom().AsTime().UTC()
	to := req.GetTo().AsTime().UTC()
	if to.Before(from) {
		return nil, status.Error(codes.InvalidArgument, "to не может быть раньше from")
	}
	limit := chpkg.ClampLimit(int(req.GetLimit()), listCandlesDefaultLimit, listCandlesMaxLimit)
	newestFirst := req.GetNewestFirst()
	// Старые grpc-web клиенты не умеют newest_first. Широкое окно + limit = догрузка с конца.
	if !newestFirst && to.Sub(from) >= 14*24*time.Hour {
		newestFirst = true
	}
	order := "ASC"
	if newestFirst {
		order = "DESC"
	}

	query := fmt.Sprintf(`
SELECT
	time,
	open,
	high,
	low,
	close,
	volume,
	volume_buy,
	volume_sell,
	candle_source,
	is_complete
FROM TrB.hct FINAL
WHERE uid = $1
	AND interval = $2
	AND time >= $3
	AND time <= $4
ORDER BY time %s
LIMIT $5`, order)

	var rows []historicCandleRow
	if err := s.db(ctx).Select(ctx, &rows, query, uid, req.GetInterval(), from, to, uint64(limit)); err != nil {
		s.log.Error().Err(err).Str("uid", uid).Int32("interval", req.GetInterval()).Msg("не удалось загрузить свечи")
		return nil, status.Errorf(codes.Internal, "не удалось загрузить свечи: %v", err)
	}

	items := make([]*chmgr.HistoricCandleRow, 0, len(rows))
	for i := range rows {
		row := &rows[i]
		items = append(items, &chmgr.HistoricCandleRow{
			Time:         chpkg.PbTime(row.Time),
			Open:         row.Open,
			High:         row.High,
			Low:          row.Low,
			Close:        row.Close,
			Volume:       row.Volume,
			VolumeBuy:    row.VolumeBuy,
			VolumeSell:   row.VolumeSell,
			CandleSource: row.CandleSource,
			IsComplete:   row.IsComplete,
		})
	}
	if newestFirst {
		for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
			items[i], items[j] = items[j], items[i]
		}
	}
	s.log.Info().
		Str("uid", uid).
		Int32("interval", req.GetInterval()).
		Int("count", len(items)).
		Bool("newest_first", newestFirst).
		Msg("исторические свечи получены")
	return &chmgr.ListCandlesResponse{Items: items}, nil
}
