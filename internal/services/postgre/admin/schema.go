package admin

import (
	"context"
	"strconv"
	"strings"

	pgpkg "github.com/Mar1eena/TrB_V3/internal/services/postgre/pkg"
	pgapi "github.com/Mar1eena/trb_proto/gen/go/postgresql"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (a *Admin) CreateSchema(ctx context.Context, req *pgapi.SchemaSpec) (*pgapi.Status, error) {
	sql, err := pgpkg.CreateSchemaSQL(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	out, err := a.exec(ctx, pool, sql)
	if err != nil {
		a.log.Error().Err(err).Str("schema", req.GetName()).Msg("не удалось создать схему")
		return nil, err
	}
	a.log.Info().Str("database", req.GetDatabase()).Str("schema", req.GetName()).Msg("схема создана")
	return out, nil
}

func (a *Admin) DropSchema(ctx context.Context, req *pgapi.SchemaName) (*pgapi.Status, error) {
	sql, err := pgpkg.DropSchemaSQL(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	out, err := a.exec(ctx, pool, sql)
	if err != nil {
		a.log.Error().Err(err).Str("schema", req.GetName()).Msg("не удалось удалить схему")
		return nil, err
	}
	a.log.Info().Str("database", req.GetDatabase()).Str("schema", req.GetName()).Msg("схема удалена")
	return out, nil
}

func (a *Admin) ListSchemas(ctx context.Context, req *pgapi.ListSchemasRequest) (*pgapi.SchemaList, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if err := requireDB(req.GetDatabase()); err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	q := `
SELECT
	n.nspname,
	pg_catalog.pg_get_userbyid(n.nspowner),
	count(c.oid) FILTER (WHERE c.relkind IN ('r', 'p')),
	coalesce(sum(pg_catalog.pg_total_relation_size(c.oid)) FILTER (WHERE c.relkind IN ('r', 'p', 'm')), 0)
FROM pg_catalog.pg_namespace n
LEFT JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid
WHERE true`
	args := make([]any, 0, 2)
	narg := 1
	if !req.GetIncludeSystem() {
		q += ` AND n.nspname NOT IN ('pg_catalog', 'information_schema')
AND n.nspname NOT LIKE 'pg_%'`
	}
	if like := strings.TrimSpace(req.GetLike()); like != "" {
		q += ` AND n.nspname LIKE $` + strconv.Itoa(narg)
		args = append(args, like)
		narg++
	}
	q += ` GROUP BY n.nspname, n.nspowner ORDER BY n.nspname`
	items, err := a.scanSchemas(ctx, pool, req.GetDatabase(), q, args...)
	if err != nil {
		a.log.Error().Err(err).Msg("не удалось получить список схем")
		return nil, err
	}
	a.log.Info().Str("database", req.GetDatabase()).Int("count", len(items)).Msg("список схем получен")
	return &pgapi.SchemaList{Items: items}, nil
}

func (a *Admin) SchemaInfo(ctx context.Context, req *pgapi.SchemaName) (*pgapi.Schema, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if err := requireDB(req.GetDatabase()); err != nil {
		return nil, err
	}
	name, err := pgpkg.Ident(req.GetName(), "name")
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	items, err := a.scanSchemas(ctx, pool, req.GetDatabase(), `
SELECT
	n.nspname,
	pg_catalog.pg_get_userbyid(n.nspowner),
	count(c.oid) FILTER (WHERE c.relkind IN ('r', 'p')),
	coalesce(sum(pg_catalog.pg_total_relation_size(c.oid)) FILTER (WHERE c.relkind IN ('r', 'p', 'm')), 0)
FROM pg_catalog.pg_namespace n
LEFT JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid
WHERE n.nspname = $1
GROUP BY n.nspname, n.nspowner`, name)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, status.Errorf(codes.NotFound, "схема %s не найдена", name)
	}
	a.log.Info().Str("schema", name).Msg("информация о схеме получена")
	return items[0], nil
}

func (a *Admin) scanSchemas(ctx context.Context, pool *pgxpool.Pool, database, query string, args ...any) ([]*pgapi.Schema, error) {
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, pgpkg.MapErr(err)
	}
	defer rows.Close()
	items := make([]*pgapi.Schema, 0)
	for rows.Next() {
		item := &pgapi.Schema{Database: database}
		var tables, bytes int64
		if err := rows.Scan(&item.Name, &item.Owner, &tables, &bytes); err != nil {
			return nil, pgpkg.MapErr(err)
		}
		item.TablesCount = u64(tables)
		item.TotalBytes = u64(bytes)
		items = append(items, item)
	}
	return items, pgpkg.MapErr(rows.Err())
}
