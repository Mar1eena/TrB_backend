package server

import (
	"context"
	"strings"

	chmgr "github.com/Mar1eena/trb_proto/gen/go/clickhouse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type databaseRow struct {
	Name    string `ch:"name"`
	Engine  string `ch:"engine"`
	Comment string `ch:"comment"`
}

type dbStatRow struct {
	Database    string  `ch:"database"`
	TablesCount uint64  `ch:"cnt"`
	TotalBytes  *uint64 `ch:"bytes"`
	TotalRows   *uint64 `ch:"rows"`
}

func (s *Server) CreateDatabase(ctx context.Context, req *chmgr.DatabaseSpec) (*chmgr.Status, error) {
	sql, err := CreateDatabaseSQL(req)
	if err != nil {
		return nil, err
	}
	out, err := s.exec(ctx, sql)
	if err != nil {
		s.log.Error().Err(err).Str("name", req.GetName()).Msg("не удалось создать базу")
		return nil, err
	}
	s.log.Info().Str("name", req.GetName()).Msg("база создана")
	return out, nil
}

func (s *Server) DropDatabase(ctx context.Context, req *chmgr.DatabaseName) (*chmgr.Status, error) {
	sql, err := DropDatabaseSQL(req)
	if err != nil {
		return nil, err
	}
	out, err := s.exec(ctx, sql)
	if err != nil {
		s.log.Error().Err(err).Str("name", req.GetName()).Msg("не удалось удалить базу")
		return nil, err
	}
	s.log.Info().Str("name", req.GetName()).Msg("база удалена")
	return out, nil
}

func (s *Server) ListDatabases(ctx context.Context, req *chmgr.ListDatabasesRequest) (*chmgr.DatabaseList, error) {
	if req == nil {
		req = &chmgr.ListDatabasesRequest{}
	}
	query := `SELECT name, engine, comment FROM system.databases`
	args := make([]any, 0, 1)
	if like := strings.TrimSpace(req.GetLike()); like != "" {
		query += ` WHERE name LIKE $1`
		args = append(args, like)
	}
	query += ` ORDER BY name`
	var rows []databaseRow
	if err := s.ch.Select(ctx, &rows, query, args...); err != nil {
		s.log.Error().Err(err).Msg("не удалось получить список баз")
		return nil, mapErr(err)
	}

	var stats []dbStatRow
	_ = s.ch.Select(ctx, &stats, `
SELECT
	database,
	count() AS cnt,
	sum(total_bytes) AS bytes,
	sum(total_rows) AS rows
FROM system.tables
GROUP BY database`)

	statsMap := make(map[string]dbStatRow, len(stats))
	for _, st := range stats {
		statsMap[st.Database] = st
	}

	items := make([]*chmgr.Database, 0, len(rows))
	for i := range rows {
		item := &chmgr.Database{
			Name:    rows[i].Name,
			Engine:  rows[i].Engine,
			Comment: rows[i].Comment,
		}
		if st, ok := statsMap[rows[i].Name]; ok {
			item.TablesCount = st.TablesCount
			if st.TotalBytes != nil {
				item.TotalBytes = *st.TotalBytes
			}
			if st.TotalRows != nil {
				item.TotalRows = *st.TotalRows
			}
		}
		items = append(items, item)
	}
	s.log.Info().Int("count", len(items)).Msg("список баз получен")
	return &chmgr.DatabaseList{Items: items}, nil
}

func (s *Server) DatabaseInfo(ctx context.Context, req *chmgr.DatabaseName) (*chmgr.Database, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	name, err := ident(req.GetName(), "name")
	if err != nil {
		return nil, err
	}
	var rows []databaseRow
	if err := s.ch.Select(ctx, &rows, `SELECT name, engine, comment FROM system.databases WHERE name = $1`, name); err != nil {
		s.log.Error().Err(err).Str("name", name).Msg("не удалось получить базу")
		return nil, mapErr(err)
	}
	if len(rows) == 0 {
		return nil, status.Errorf(codes.NotFound, "база %s не найдена", name)
	}

	var stats []dbStatRow
	_ = s.ch.Select(ctx, &stats, `
SELECT
	database,
	count() AS cnt,
	sum(total_bytes) AS bytes,
	sum(total_rows) AS rows
FROM system.tables
WHERE database = $1
GROUP BY database`, name)

	res := &chmgr.Database{
		Name:    rows[0].Name,
		Engine:  rows[0].Engine,
		Comment: rows[0].Comment,
	}
	if len(stats) > 0 {
		res.TablesCount = stats[0].TablesCount
		if stats[0].TotalBytes != nil {
			res.TotalBytes = *stats[0].TotalBytes
		}
		if stats[0].TotalRows != nil {
			res.TotalRows = *stats[0].TotalRows
		}
	}

	s.log.Info().Str("name", name).Msg("информация о базе получена")
	return res, nil
}
