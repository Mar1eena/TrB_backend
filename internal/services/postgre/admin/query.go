package admin

import (
	"context"
	"strings"
	"time"

	pgpkg "github.com/Mar1eena/TrB_V3/internal/services/postgre/pkg"
	pgapi "github.com/Mar1eena/trb_proto/gen/go/postgresql"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (a *Admin) ExecuteQuery(ctx context.Context, req *pgapi.ExecuteQueryRequest) (*pgapi.ExecuteQueryResponse, error) {
	if req == nil || strings.TrimSpace(req.GetQuery()) == "" {
		return nil, status.Error(codes.InvalidArgument, "текст запроса обязателен")
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	maxRows := pgpkg.ClampLimit(int(req.GetMaxRows()), 100, 1000)
	out, err := a.runQuery(ctx, pool, strings.TrimSpace(req.GetQuery()), maxRows)
	if err != nil {
		a.log.Error().Err(err).Msg("не удалось выполнить SQL")
		return nil, err
	}
	a.log.Info().Uint64("rows", out.GetTotalRows()).Float64("elapsed_s", out.GetElapsedSeconds()).Msg("SQL выполнен")
	return out, nil
}

func (a *Admin) PreviewTableData(ctx context.Context, req *pgapi.PreviewTableDataRequest) (*pgapi.ExecuteQueryResponse, error) {
	sql, err := pgpkg.PreviewTableDataSQL(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	maxRows := pgpkg.ClampLimit(int(req.GetLimit()), 50, 500)
	out, err := a.runQuery(ctx, pool, sql, maxRows)
	if err != nil {
		a.log.Error().Err(err).Msg("не удалось получить превью таблицы")
		return nil, err
	}
	return out, nil
}

func (a *Admin) runQuery(ctx context.Context, pool *pgxpool.Pool, q string, maxRows int) (*pgapi.ExecuteQueryResponse, error) {
	start := time.Now()
	rows, err := pool.Query(ctx, q)
	if err != nil {
		tag, err2 := a.simpleExec(ctx, pool, q)
		if err2 != nil {
			return nil, pgpkg.MapErr(err)
		}
		return &pgapi.ExecuteQueryResponse{
			ElapsedSeconds: elapsed(start),
			RowsAffected:   u64(tag.RowsAffected()),
		}, nil
	}
	defer rows.Close()

	fds := rows.FieldDescriptions()
	typeMap := pgtype.NewMap()
	cols := make([]string, len(fds))
	types := make([]string, len(fds))
	for i, fd := range fds {
		cols[i] = fd.Name
		if t, ok := typeMap.TypeForOID(fd.DataTypeOID); ok {
			types[i] = t.Name
		}
	}

	result := make([]*pgapi.QueryRow, 0, maxRows)
	var total uint64
	for rows.Next() {
		if len(result) >= maxRows {
			break
		}
		total++
		vals, err := rows.Values()
		if err != nil {
			a.log.Warn().Err(err).Msg("ошибка сканирования строки PostgreSQL")
			continue
		}
		str := make([]string, len(vals))
		for i, v := range vals {
			str[i] = pgpkg.FormatCellValue(v)
		}
		result = append(result, &pgapi.QueryRow{Values: str})
	}
	if err := rows.Err(); err != nil {
		return nil, pgpkg.MapErr(err)
	}
	tag := rows.CommandTag()
	return &pgapi.ExecuteQueryResponse{
		Columns:        cols,
		Types:          types,
		Rows:           result,
		TotalRows:      total,
		ElapsedSeconds: elapsed(start),
		RowsAffected:   u64(tag.RowsAffected()),
	}, nil
}
