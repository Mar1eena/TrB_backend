package server

import (
	"context"
	"strings"

	chdb "github.com/Mar1eena/TrB_V3/internal/pkg/db/clickhouse"
	instrpkg "github.com/Mar1eena/TrB_V3/internal/services/instruments/pkg"
	instrpb "github.com/Mar1eena/trb_proto/gen/go/instruments"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) ListInstrumentVersions(ctx context.Context, req *instrpb.ListInstrumentVersionsRequest) (*instrpb.ListInstrumentVersionsResponse, error) {
	if req == nil {
		req = &instrpb.ListInstrumentVersionsRequest{}
	}
	uid := strings.TrimSpace(req.GetUid())
	if uid == "" {
		return nil, status.Error(codes.InvalidArgument, "uid обязателен")
	}

	// Без FINAL — все незамёрженные версии ReplacingMergeTree.
	query := `
SELECT ` + chdb.ShtSelectColumns + `
FROM TrB.sht
WHERE uid = $1
ORDER BY version DESC`

	var rows []instrpkg.InstrumentRow
	if err := s.ch.Select(ctx, &rows, query, uid); err != nil {
		s.log.Error().Err(err).Str("uid", uid).Msg("не удалось загрузить версии инструмента")
		return nil, status.Errorf(codes.Internal, "не удалось загрузить версии: %v", err)
	}

	items := make([]*instrpb.InstrumentVersion, 0, len(rows))
	seen := make(map[int64]struct{}, len(rows))
	for i := range rows {
		key := rows[i].Version.UTC().UnixMilli()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		items = append(items, &instrpb.InstrumentVersion{
			Share:   instrpkg.InstrumentFromRow(&rows[i], false),
			Version: chdb.PbTime(rows[i].Version),
		})
	}

	s.log.Info().Str("uid", uid).Int("count", len(items)).Msg("версии инструмента загружены")
	return &instrpb.ListInstrumentVersionsResponse{Items: items}, nil
}
