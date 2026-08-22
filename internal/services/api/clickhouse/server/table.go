package server

import (
	"context"
	"strings"
	"time"

	chmgr "github.com/Mar1eena/trb_proto/gen/go/clickhouse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type tableRow struct {
	Database                 string    `ch:"database"`
	Name                     string    `ch:"name"`
	Engine                   string    `ch:"engine"`
	TotalRows                *uint64   `ch:"total_rows"`
	TotalBytes               *uint64   `ch:"total_bytes"`
	PartitionKey             string    `ch:"partition_key"`
	SortingKey               string    `ch:"sorting_key"`
	PrimaryKey               string    `ch:"primary_key"`
	Comment                  string    `ch:"comment"`
	CreateTableQuery         string    `ch:"create_table_query"`
	MetadataModificationTime time.Time `ch:"metadata_modification_time"`
}

type describeColumnRow struct {
	Name              string `ch:"name"`
	Type              string `ch:"type"`
	DefaultKind       string `ch:"default_type"`
	DefaultExpression string `ch:"default_expression"`
	CompressionCodec  string `ch:"codec_expression"`
	TTLExpression     string `ch:"ttl_expression"`
	Comment           string `ch:"comment"`
}

type systemColumnRow struct {
	Name              string `ch:"name"`
	Type              string `ch:"type"`
	DefaultKind       string `ch:"default_kind"`
	DefaultExpression string `ch:"default_expression"`
	CompressionCodec  string `ch:"compression_codec"`
	Comment           string `ch:"comment"`
}

func columnFromDescribe(row *describeColumnRow) *chmgr.Column {
	return &chmgr.Column{
		Name:              row.Name,
		Type:              row.Type,
		DefaultKind:       row.DefaultKind,
		DefaultExpression: row.DefaultExpression,
		Codec:             row.CompressionCodec,
		Ttl:               row.TTLExpression,
		Comment:           row.Comment,
	}
}

func columnFromSystem(row *systemColumnRow) *chmgr.Column {
	return &chmgr.Column{
		Name:              row.Name,
		Type:              row.Type,
		DefaultKind:       row.DefaultKind,
		DefaultExpression: row.DefaultExpression,
		Codec:             row.CompressionCodec,
		Comment:           row.Comment,
	}
}

func tableFromRow(row *tableRow, cols []*chmgr.Column) *chmgr.Table {
	out := &chmgr.Table{
		Database:         row.Database,
		Name:             row.Name,
		Engine:           row.Engine,
		PartitionKey:     row.PartitionKey,
		SortingKey:       row.SortingKey,
		PrimaryKey:       row.PrimaryKey,
		Comment:          row.Comment,
		CreateTableQuery: row.CreateTableQuery,
		Columns:          cols,
	}
	if row.TotalRows != nil {
		out.TotalRows = *row.TotalRows
	}
	if row.TotalBytes != nil {
		out.TotalBytes = *row.TotalBytes
	}
	if !row.MetadataModificationTime.IsZero() && row.MetadataModificationTime.Unix() > 0 {
		out.MetadataModificationTime = timestamppb.New(row.MetadataModificationTime.UTC())
	}
	return out
}

func (s *Server) CreateTable(ctx context.Context, req *chmgr.TableSpec) (*chmgr.Status, error) {
	sql, err := CreateTableSQL(req)
	if err != nil {
		return nil, err
	}
	out, err := s.exec(ctx, sql)
	if err != nil {
		s.log.Error().Err(err).Str("database", req.GetDatabase()).Str("name", req.GetName()).Msg("не удалось создать таблицу")
		return nil, err
	}
	s.log.Info().Str("database", req.GetDatabase()).Str("name", req.GetName()).Msg("таблица создана")
	return out, nil
}

func (s *Server) DropTable(ctx context.Context, req *chmgr.TableName) (*chmgr.Status, error) {
	sql, err := dropTableSQL(req)
	if err != nil {
		return nil, err
	}
	out, err := s.exec(ctx, sql)
	if err != nil {
		s.log.Error().Err(err).Str("database", req.GetDatabase()).Str("name", req.GetName()).Msg("не удалось удалить таблицу")
		return nil, err
	}
	s.log.Info().Str("database", req.GetDatabase()).Str("name", req.GetName()).Msg("таблица удалена")
	return out, nil
}

func (s *Server) TruncateTable(ctx context.Context, req *chmgr.TableName) (*chmgr.Status, error) {
	sql, err := truncateTableSQL(req)
	if err != nil {
		return nil, err
	}
	out, err := s.exec(ctx, sql)
	if err != nil {
		s.log.Error().Err(err).Str("database", req.GetDatabase()).Str("name", req.GetName()).Msg("не удалось очистить таблицу")
		return nil, err
	}
	s.log.Info().Str("database", req.GetDatabase()).Str("name", req.GetName()).Msg("таблица очищена")
	return out, nil
}

func (s *Server) RenameTable(ctx context.Context, req *chmgr.RenameTableRequest) (*chmgr.Status, error) {
	sql, err := RenameTableSQL(req)
	if err != nil {
		return nil, err
	}
	out, err := s.exec(ctx, sql)
	if err != nil {
		s.log.Error().Err(err).Str("database", req.GetDatabase()).Str("name", req.GetName()).Msg("не удалось переименовать таблицу")
		return nil, err
	}
	s.log.Info().Str("name", req.GetName()).Str("new_name", req.GetNewName()).Msg("таблица переименована")
	return out, nil
}

func (s *Server) OptimizeTable(ctx context.Context, req *chmgr.OptimizeTableRequest) (*chmgr.Status, error) {
	sql, err := OptimizeTableSQL(req)
	if err != nil {
		return nil, err
	}
	out, err := s.exec(ctx, sql)
	if err != nil {
		s.log.Error().Err(err).Str("database", req.GetDatabase()).Str("name", req.GetName()).Msg("не удалось оптимизировать таблицу")
		return nil, err
	}
	s.log.Info().Str("database", req.GetDatabase()).Str("name", req.GetName()).Msg("таблица оптимизирована")
	return out, nil
}

func (s *Server) ListTables(ctx context.Context, req *chmgr.ListTablesRequest) (*chmgr.TableList, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	database, err := ident(req.GetDatabase(), "database")
	if err != nil {
		return nil, err
	}
	query := `
SELECT
	database, name, engine,
	total_rows, total_bytes,
	partition_key, sorting_key, primary_key,
	comment, create_table_query,
	metadata_modification_time
FROM system.tables
WHERE database = $1`
	args := []any{database}
	if like := strings.TrimSpace(req.GetLike()); like != "" {
		query += ` AND name LIKE $2`
		args = append(args, like)
	}
	query += ` ORDER BY name`
	var rows []tableRow
	if err := s.ch.Select(ctx, &rows, query, args...); err != nil {
		s.log.Error().Err(err).Str("database", database).Msg("не удалось получить список таблиц")
		return nil, mapErr(err)
	}
	items := make([]*chmgr.Table, 0, len(rows))
	for i := range rows {
		items = append(items, tableFromRow(&rows[i], nil))
	}
	s.log.Info().Str("database", database).Int("count", len(items)).Msg("список таблиц получен")
	return &chmgr.TableList{Items: items}, nil
}

func (s *Server) TableInfo(ctx context.Context, req *chmgr.TableName) (*chmgr.Table, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	database, err := ident(req.GetDatabase(), "database")
	if err != nil {
		return nil, err
	}
	name, err := ident(req.GetName(), "name")
	if err != nil {
		return nil, err
	}
	var rows []tableRow
	err = s.ch.Select(ctx, &rows, `
SELECT
	database, name, engine,
	total_rows, total_bytes,
	partition_key, sorting_key, primary_key,
	comment, create_table_query,
	metadata_modification_time
FROM system.tables
WHERE database = $1 AND name = $2`, database, name)
	if err != nil {
		s.log.Error().Err(err).Str("database", database).Str("name", name).Msg("не удалось получить таблицу")
		return nil, mapErr(err)
	}
	if len(rows) == 0 {
		return nil, status.Errorf(codes.NotFound, "таблица %s.%s не найдена", database, name)
	}
	tbl, err := qualified(database, name, "database", "name")
	if err != nil {
		return nil, err
	}
	items, err := s.listColumns(ctx, tbl, database, name)
	if err != nil {
		s.log.Error().Err(err).Str("database", database).Str("name", name).Msg("не удалось получить колонки")
		return nil, mapErr(err)
	}

	var partStats []struct {
		Count uint64  `ch:"cnt"`
		Bytes *uint64 `ch:"bytes"`
	}
	_ = s.ch.Select(ctx, &partStats, `
SELECT
	count() AS cnt,
	sum(data_uncompressed_bytes) AS bytes
FROM system.parts
WHERE database = $1 AND table = $2 AND active = 1`, database, name)

	tblInfo := tableFromRow(&rows[0], items)
	if len(partStats) > 0 {
		tblInfo.PartsCount = partStats[0].Count
		if partStats[0].Bytes != nil {
			tblInfo.DataUncompressedBytes = *partStats[0].Bytes
		}
	}

	s.log.Info().Str("database", database).Str("name", name).Int("columns", len(items)).Msg("информация о таблице получена")
	return tblInfo, nil
}

func (s *Server) listColumns(ctx context.Context, tbl, database, name string) ([]*chmgr.Column, error) {
	var described []describeColumnRow
	if err := s.ch.Select(ctx, &described, "DESCRIBE TABLE "+tbl); err == nil {
		items := make([]*chmgr.Column, 0, len(described))
		for i := range described {
			items = append(items, columnFromDescribe(&described[i]))
		}
		return items, nil
	}
	var cols []systemColumnRow
	if err := s.ch.Select(ctx, &cols, `
SELECT
	name, type, default_kind, default_expression,
	compression_codec, comment
FROM system.columns
WHERE database = $1 AND table = $2
ORDER BY position`, database, name); err != nil {
		return nil, err
	}
	items := make([]*chmgr.Column, 0, len(cols))
	for i := range cols {
		items = append(items, columnFromSystem(&cols[i]))
	}
	return items, nil
}
