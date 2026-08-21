package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	chmgr "github.com/Mar1eena/trb_proto/gen/go/api/clickhouse"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type partRow struct {
	Partition             string    `ch:"partition"`
	Name                  string    `ch:"name"`
	Active                uint8     `ch:"active"`
	Rows                  uint64    `ch:"rows"`
	BytesOnDisk           uint64    `ch:"bytes_on_disk"`
	DataUncompressedBytes uint64    `ch:"data_uncompressed_bytes"`
	ModificationTime      time.Time `ch:"modification_time"`
	DiskName              string    `ch:"disk_name"`
	MinDate               string    `ch:"min_date"`
	MaxDate               string    `ch:"max_date"`
}

func (s *Server) ListParts(ctx context.Context, req *chmgr.ListPartsRequest) (*chmgr.PartsList, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	db, err := ident(req.GetDatabase(), "database")
	if err != nil {
		return nil, err
	}
	tbl, err := ident(req.GetTable(), "table")
	if err != nil {
		return nil, err
	}

	query := `
SELECT
	partition,
	name,
	active,
	rows,
	bytes_on_disk,
	data_uncompressed_bytes,
	modification_time,
	disk_name,
	toString(min_date) AS min_date,
	toString(max_date) AS max_date
FROM system.parts
WHERE database = $1 AND table = $2`

	if req.GetActiveOnly() {
		query += " AND active = 1"
	}
	query += " ORDER BY modification_time DESC, name"

	var rows []partRow
	if err := s.ch.Select(ctx, &rows, query, db, tbl); err != nil {
		s.log.Error().Err(err).Str("database", db).Str("table", tbl).Msg("не удалось получить партиции")
		return nil, mapErr(err)
	}

	items := make([]*chmgr.TablePart, 0, len(rows))
	for i := range rows {
		p := &chmgr.TablePart{
			Partition:             rows[i].Partition,
			Name:                  rows[i].Name,
			Active:                rows[i].Active == 1,
			Rows:                  rows[i].Rows,
			BytesOnDisk:           rows[i].BytesOnDisk,
			DataUncompressedBytes: rows[i].DataUncompressedBytes,
			DiskName:              rows[i].DiskName,
			MinDate:               rows[i].MinDate,
			MaxDate:               rows[i].MaxDate,
		}
		if !rows[i].ModificationTime.IsZero() && rows[i].ModificationTime.Unix() > 0 {
			p.ModificationTime = timestamppb.New(rows[i].ModificationTime.UTC())
		}
		items = append(items, p)
	}

	s.log.Info().Str("database", db).Str("table", tbl).Int("count", len(items)).Msg("партиции получены")
	return &chmgr.PartsList{Items: items}, nil
}

func (s *Server) DropPartition(ctx context.Context, req *chmgr.DropPartitionRequest) (*chmgr.Status, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	db, err := ident(req.GetDatabase(), "database")
	if err != nil {
		return nil, err
	}
	tbl, err := ident(req.GetTable(), "table")
	if err != nil {
		return nil, err
	}
	part := strings.TrimSpace(req.GetPartition())
	if part == "" {
		return nil, status.Error(codes.InvalidArgument, "имя/ID партиции обязательно")
	}

	action := "DROP PARTITION"
	if req.GetDetach() {
		action = "DETACH PARTITION"
	}

	var sql string
	if cluster := strings.TrimSpace(req.GetCluster()); cluster != "" {
		cName, err := ident(cluster, "cluster")
		if err != nil {
			return nil, err
		}
		sql = fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` %s %s", db, tbl, cName, action, quotePartition(part))
	} else {
		sql = fmt.Sprintf("ALTER TABLE `%s`.`%s` %s %s", db, tbl, action, quotePartition(part))
	}

	out, err := s.exec(ctx, sql)
	if err != nil {
		s.log.Error().Err(err).Str("database", db).Str("table", tbl).Str("partition", part).Msg("не удалось выполнить операцию с партицией")
		return nil, err
	}

	s.log.Info().Str("database", db).Str("table", tbl).Str("partition", part).Msg("операция с партицией выполнена успешно")
	return out, nil
}

func quotePartition(p string) string {
	// If it starts and ends with parentheses or is numeric/tuple expression, or already quoted
	p = strings.TrimSpace(p)
	if strings.HasPrefix(p, "(") && strings.HasSuffix(p, ")") {
		return p
	}
	if strings.HasPrefix(p, "'") && strings.HasSuffix(p, "'") {
		return p
	}
	// If ID '...'
	if strings.HasPrefix(strings.ToUpper(p), "ID ") {
		return p
	}
	// Check if integer
	if _, err := fmt.Sscanf(p, "%d", new(int64)); err == nil {
		return p
	}
	// Otherwise quote string
	return quote(p)
}
