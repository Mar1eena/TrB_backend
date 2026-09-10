package server

import (
	"context"
	"fmt"

	"github.com/Mar1eena/TrB_V3/internal/pkg/db/clickhouse"
	chpkg "github.com/Mar1eena/TrB_V3/internal/services/clickhouse/pkg"
	chmgr "github.com/Mar1eena/trb_proto/gen/go/clickhouse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) ListInstruments(ctx context.Context, req *chmgr.ListInstrumentsRequest) (*chmgr.ListInstrumentsResponse, error) {
	if req == nil {
		req = &chmgr.ListInstrumentsRequest{}
	}
	q, limit, offset := chpkg.FilterFrom(req.GetFilter(), 2000, 20000)
	lite := req.GetLite()

	clause, searchArgs, next := chpkg.SearchClause(q, "", 1)
	query := fmt.Sprintf(`
SELECT %s
FROM TrB.sht FINAL
WHERE %s
ORDER BY ticker ASC
LIMIT $%d OFFSET $%d`, clickhouse.ShtSelectColumns, clause, next, next+1)

	args := append(searchArgs, uint64(limit), uint64(offset))
	var rows []chpkg.InstrumentRow
	if err := s.db(ctx).Select(ctx, &rows, query, args...); err != nil {
		s.log.Error().Err(err).Str("q", q).Msg("не удалось загрузить инструменты")
		return nil, status.Errorf(codes.Internal, "не удалось загрузить инструменты: %v", err)
	}

	counts := map[string]int32{}
	if !lite {
		counts = s.versionCounts(ctx)
	}
	items := make([]*chmgr.InstrumentListItem, 0, len(rows))
	for i := range rows {
		count := counts[rows[i].UID]
		if count == 0 {
			count = 1
		}
		items = append(items, &chmgr.InstrumentListItem{
			Share:        chpkg.InstrumentFromRow(&rows[i], lite),
			Version:      chpkg.PbTime(rows[i].Version),
			VersionCount: count,
		})
	}
	s.log.Info().Int("count", len(items)).Bool("lite", lite).Str("q", q).Msg("инструменты загружены")
	return &chmgr.ListInstrumentsResponse{Items: items}, nil
}

type versionCountRow struct {
	UID          string `ch:"uid"`
	VersionCount uint64 `ch:"version_count"`
}

// versionCounts — агрегация по всей таблице без IN(...), чтобы не раздувать max_query_size.
func (s *Server) versionCounts(ctx context.Context) map[string]int32 {
	out := make(map[string]int32)
	var counts []versionCountRow
	err := s.db(ctx).Select(ctx, &counts, `
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
