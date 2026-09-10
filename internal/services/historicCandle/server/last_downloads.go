package server

import (
	"context"
	"fmt"
	"time"

	chdb "github.com/Mar1eena/TrB_V3/internal/pkg/db/clickhouse"
	hcpb "github.com/Mar1eena/trb_proto/gen/go/historiccandle"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type lastDownloadRow struct {
	UID         string    `ch:"uid"`
	Figi        string    `ch:"figi"`
	Ticker      string    `ch:"ticker"`
	Name        string    `ch:"name"`
	Interval    int32     `ch:"interval"`
	LastStart   time.Time `ch:"last_start"`
	LastEnd     time.Time `ch:"last_end"`
	HasDownload uint8     `ch:"has_download"`
}

func (s *Server) ListLastDownloads(ctx context.Context, req *hcpb.ListLastDownloadsRequest) (*hcpb.ListLastDownloadsResponse, error) {
	if req == nil {
		req = &hcpb.ListLastDownloadsRequest{}
	}
	f := req.GetFilter()
	q, limit, offset := chdb.FilterFrom(f.GetQ(), int(f.GetLimit()), int(f.GetOffset()), 500, 5000)
	clause, searchArgs, next := chdb.SearchClause(q, "", 1)

	query := fmt.Sprintf(`
SELECT
	sht.uid AS uid,
	sht.figi AS figi,
	sht.ticker AS ticker,
	sht.name AS name,
	ld.interval AS interval,
	ld.last_start AS last_start,
	ld.last_end AS last_end,
	toUInt8(1) AS has_download
FROM (
	SELECT uid, figi, ticker, name
	FROM TrB.sht FINAL
	WHERE %s
) AS sht
INNER JOIN (
	SELECT
		uid,
		interval,
		max(download_start) AS last_start,
		max(download_end) AS last_end
	FROM TrB.hct_last_download
	GROUP BY uid, interval
) AS ld ON sht.uid = ld.uid
ORDER BY last_start DESC
LIMIT $%d OFFSET $%d`, clause, next, next+1)

	args := append(searchArgs, uint64(limit), uint64(offset))
	var rows []lastDownloadRow
	if err := s.ch.Select(ctx, &rows, query, args...); err != nil {
		s.log.Error().Err(err).Str("q", q).Msg("не удалось загрузить историю загрузок")
		return nil, status.Errorf(codes.Internal, "не удалось загрузить историю загрузок: %v", err)
	}

	items := make([]*hcpb.LastDownload, 0, len(rows))
	for i := range rows {
		row := &rows[i]
		items = append(items, &hcpb.LastDownload{
			Uid:         row.UID,
			Figi:        row.Figi,
			Ticker:      row.Ticker,
			Name:        row.Name,
			Interval:    row.Interval,
			LastStart:   chdb.PbTime(row.LastStart),
			LastEnd:     chdb.PbTime(row.LastEnd),
			HasDownload: row.HasDownload != 0,
		})
	}
	s.log.Info().Int("count", len(items)).Str("q", q).Msg("история загрузок получена")
	return &hcpb.ListLastDownloadsResponse{Items: items}, nil
}
