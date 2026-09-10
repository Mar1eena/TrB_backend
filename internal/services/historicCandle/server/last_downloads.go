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

// ldSortCols — whitelist колонок сортировки истории загрузок.
var ldSortCols = map[string]string{
	"uid": "uid", "name": "name", "ticker": "ticker", "interval": "interval",
	"last_start": "last_start", "last_end": "last_end",
}

// ldFilterCols — whitelist колонок подстрочных фильтров (все из подзапроса sht).
var ldFilterCols = map[string]string{
	"ticker": "ticker", "name": "name", "uid": "uid",
}

func ldFieldFilterInputs(pb []*hcpb.FieldFilter) []chdb.FieldFilterInput {
	out := make([]chdb.FieldFilterInput, 0, len(pb))
	for _, f := range pb {
		out = append(out, chdb.FieldFilterInput{Field: f.GetField(), Value: f.GetValue()})
	}
	return out
}

func (s *Server) ListLastDownloads(ctx context.Context, req *hcpb.ListLastDownloadsRequest) (*hcpb.ListLastDownloadsResponse, error) {
	if req == nil {
		req = &hcpb.ListLastDownloadsRequest{}
	}
	f := req.GetFilter()
	q, limit, offset := chdb.FilterFrom(f.GetQ(), int(f.GetLimit()), int(f.GetOffset()), 500, 5000)
	searchClause, searchArgs, next := chdb.SearchClause(q, "", 1)
	fieldClause, fieldArgs, next := chdb.FieldFiltersClause(ldFieldFilterInputs(f.GetFieldFilters()), ldFilterCols, next)
	shtWhere := searchClause + " AND " + fieldClause

	// Фильтр по интервалу применяется к результату JOIN.
	intervalClause := "true"
	joinArgs := append(append([]any{}, searchArgs...), fieldArgs...)
	if iv := f.GetIntervalFilter(); iv > 0 {
		intervalClause = fmt.Sprintf("ld.interval = $%d", next)
		joinArgs = append(joinArgs, iv)
		next++
	}

	joinSQL := fmt.Sprintf(`
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
WHERE %s`, shtWhere, intervalClause)

	var total uint64
	if err := s.ch.QueryRow(ctx, "SELECT count() "+joinSQL, joinArgs...).Scan(&total); err != nil {
		s.log.Error().Err(err).Str("q", q).Msg("не удалось посчитать историю загрузок")
		return nil, status.Errorf(codes.Internal, "не удалось посчитать историю загрузок: %v", err)
	}

	sortBy, sortDesc := f.GetSortBy(), f.GetSortDesc()
	if sortBy == "" {
		sortBy, sortDesc = "last_start", true // историческое поведение: свежие сверху
	}
	order := chdb.SortClause(sortBy, ldSortCols, "last_start", sortDesc)
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
%s
%s
LIMIT $%d OFFSET $%d`, joinSQL, order, next, next+1)

	args := append(joinArgs, uint64(limit), uint64(offset))
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
	s.log.Info().Int("count", len(items)).Int64("total", int64(total)).Str("q", q).Msg("история загрузок получена")
	return &hcpb.ListLastDownloadsResponse{Items: items, Total: int32(total)}, nil
}
