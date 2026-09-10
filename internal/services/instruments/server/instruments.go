package server

import (
	"context"
	"fmt"

	chdb "github.com/Mar1eena/TrB_V3/internal/pkg/db/clickhouse"
	instrpkg "github.com/Mar1eena/TrB_V3/internal/services/instruments/pkg"
	instrpb "github.com/Mar1eena/trb_proto/gen/go/instruments"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// instrSortCols — whitelist колонок сортировки TrB.sht.
var instrSortCols = map[string]string{
	"ticker": "ticker", "name": "name", "figi": "figi", "uid": "uid",
	"currency": "currency", "exchange": "exchange", "version": "version",
	"trading_status": "trading_status", "lot": "lot",
}

// instrFilterCols — whitelist колонок для подстрочных фильтров.
var instrFilterCols = map[string]string{
	"ticker": "ticker", "name": "name", "figi": "figi", "uid": "uid",
	"currency": "currency", "exchange": "exchange", "class_code": "class_code", "sector": "sector",
}

func fieldFilterInputs(pb []*instrpb.FieldFilter) []chdb.FieldFilterInput {
	out := make([]chdb.FieldFilterInput, 0, len(pb))
	for _, f := range pb {
		out = append(out, chdb.FieldFilterInput{Field: f.GetField(), Value: f.GetValue()})
	}
	return out
}

func (s *Server) ListInstruments(ctx context.Context, req *instrpb.ListInstrumentsRequest) (*instrpb.ListInstrumentsResponse, error) {
	if req == nil {
		req = &instrpb.ListInstrumentsRequest{}
	}
	f := req.GetFilter()
	q, limit, offset := chdb.FilterFrom(f.GetQ(), int(f.GetLimit()), int(f.GetOffset()), 2000, 20000)
	lite := req.GetLite()

	searchClause, searchArgs, next := chdb.SearchClause(q, "", 1)
	fieldClause, fieldArgs, next := chdb.FieldFiltersClause(fieldFilterInputs(f.GetFieldFilters()), instrFilterCols, next)
	whereSQL := searchClause + " AND " + fieldClause
	whereArgs := append(append([]any{}, searchArgs...), fieldArgs...)

	var total uint64
	countSQL := fmt.Sprintf("SELECT count() FROM TrB.sht FINAL WHERE %s", whereSQL)
	if err := s.ch.QueryRow(ctx, countSQL, whereArgs...).Scan(&total); err != nil {
		s.log.Error().Err(err).Str("q", q).Msg("не удалось посчитать инструменты")
		return nil, status.Errorf(codes.Internal, "не удалось посчитать инструменты: %v", err)
	}

	order := chdb.SortClause(f.GetSortBy(), instrSortCols, "ticker", f.GetSortDesc())
	query := fmt.Sprintf(`
SELECT %s
FROM TrB.sht FINAL
WHERE %s
%s
LIMIT $%d OFFSET $%d`, chdb.ShtSelectColumns, whereSQL, order, next, next+1)

	args := append(whereArgs, uint64(limit), uint64(offset))
	var rows []instrpkg.InstrumentRow
	if err := s.ch.Select(ctx, &rows, query, args...); err != nil {
		s.log.Error().Err(err).Str("q", q).Msg("не удалось загрузить инструменты")
		return nil, status.Errorf(codes.Internal, "не удалось загрузить инструменты: %v", err)
	}

	counts := map[string]int32{}
	if !lite {
		counts = s.versionCounts(ctx)
	}
	items := make([]*instrpb.InstrumentListItem, 0, len(rows))
	for i := range rows {
		count := counts[rows[i].UID]
		if count == 0 {
			count = 1
		}
		items = append(items, &instrpb.InstrumentListItem{
			Share:        instrpkg.InstrumentFromRow(&rows[i], lite),
			Version:      chdb.PbTime(rows[i].Version),
			VersionCount: count,
		})
	}
	s.log.Info().Int("count", len(items)).Int64("total", int64(total)).Bool("lite", lite).Str("q", q).Msg("инструменты загружены")
	return &instrpb.ListInstrumentsResponse{Items: items, Total: int32(total)}, nil
}

type versionCountRow struct {
	UID          string `ch:"uid"`
	VersionCount uint64 `ch:"version_count"`
}

// versionCounts — агрегация по всей таблице без IN(...), чтобы не раздувать max_query_size.
func (s *Server) versionCounts(ctx context.Context) map[string]int32 {
	out := make(map[string]int32)
	var counts []versionCountRow
	err := s.ch.Select(ctx, &counts, `
SELECT
	uid,
	uniqExact(version) AS version_count
FROM TrB.sht
GROUP BY uid`)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось посчитать версии инструментов")
		return out
	}
	for _, row := range counts {
		out[row.UID] = int32(row.VersionCount)
	}
	return out
}
