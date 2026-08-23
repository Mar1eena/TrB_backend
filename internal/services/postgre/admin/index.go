package admin

import (
	"context"

	pgpkg "github.com/Mar1eena/TrB_V3/internal/services/postgre/pkg"
	pgapi "github.com/Mar1eena/trb_proto/gen/go/postgresql"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (a *Admin) CreateIndex(ctx context.Context, req *pgapi.IndexSpec) (*pgapi.Status, error) {
	sql, err := pgpkg.CreateIndexSQL(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	out, err := a.exec(ctx, pool, sql)
	if err != nil {
		a.log.Error().Err(err).Str("index", req.GetName()).Msg("не удалось создать индекс")
		return nil, err
	}
	a.log.Info().Str("table", req.GetTable()).Str("index", req.GetName()).Msg("индекс создан")
	return out, nil
}

func (a *Admin) DropIndex(ctx context.Context, req *pgapi.IndexName) (*pgapi.Status, error) {
	sql, err := pgpkg.DropIndexSQL(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	out, err := a.exec(ctx, pool, sql)
	if err != nil {
		a.log.Error().Err(err).Str("index", req.GetName()).Msg("не удалось удалить индекс")
		return nil, err
	}
	a.log.Info().Str("index", req.GetName()).Msg("индекс удалён")
	return out, nil
}

func (a *Admin) ListIndexes(ctx context.Context, req *pgapi.ListIndexesRequest) (*pgapi.IndexList, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if err := requireDB(req.GetDatabase()); err != nil {
		return nil, err
	}
	schema, err := pgpkg.Ident(req.GetSchema(), "schema")
	if err != nil {
		return nil, err
	}
	table, err := pgpkg.Ident(req.GetTable(), "table")
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	items, err := a.scanIndexes(ctx, pool, req.GetDatabase(), schema, table)
	if err != nil {
		a.log.Error().Err(err).Str("table", table).Msg("не удалось получить список индексов")
		return nil, err
	}
	a.log.Info().Str("table", table).Int("count", len(items)).Msg("список индексов получен")
	return &pgapi.IndexList{Items: items}, nil
}

func (a *Admin) Reindex(ctx context.Context, req *pgapi.ReindexRequest) (*pgapi.Status, error) {
	sql, err := pgpkg.ReindexSQL(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	out, err := a.exec(ctx, pool, sql)
	if err != nil {
		a.log.Error().Err(err).Str("table", req.GetTable()).Msg("не удалось выполнить REINDEX")
		return nil, err
	}
	a.log.Info().Str("table", req.GetTable()).Str("index", req.GetName()).Msg("REINDEX выполнен")
	return out, nil
}

func (a *Admin) scanIndexes(ctx context.Context, pool *pgxpool.Pool, database, schema, table string) ([]*pgapi.Index, error) {
	rows, err := pool.Query(ctx, `
SELECT
	current_database(),
	n.nspname,
	t.relname,
	i.relname,
	am.amname,
	ix.indisunique,
	ix.indisprimary,
	ix.indisvalid,
	coalesce(array_agg(a.attname ORDER BY k.ord) FILTER (WHERE a.attname IS NOT NULL), '{}'),
	pg_catalog.pg_get_indexdef(ix.indexrelid),
	coalesce(pg_catalog.pg_relation_size(i.oid), 0),
	coalesce(ts.spcname, '')
FROM pg_catalog.pg_index ix
JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
JOIN pg_catalog.pg_am am ON am.oid = i.relam
LEFT JOIN pg_catalog.pg_tablespace ts ON ts.oid = nullif(i.reltablespace, 0)
LEFT JOIN unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
WHERE n.nspname = $1 AND t.relname = $2
GROUP BY n.nspname, t.relname, i.relname, am.amname, ix.indisunique, ix.indisprimary, ix.indisvalid, ix.indexrelid, i.oid, ts.spcname
ORDER BY i.relname`, schema, table)
	if err != nil {
		return nil, pgpkg.MapErr(err)
	}
	defer rows.Close()
	items := make([]*pgapi.Index, 0)
	for rows.Next() {
		var (
			item pgapi.Index
			size int64
			cols []string
		)
		if err := rows.Scan(
			&item.Database, &item.Schema, &item.Table, &item.Name, &item.Method,
			&item.Unique, &item.Primary, &item.Valid, &cols, &item.Definition, &size, &item.Tablespace,
		); err != nil {
			return nil, pgpkg.MapErr(err)
		}
		item.Columns = cols
		item.SizeBytes = u64(size)
		if item.Database == "" {
			item.Database = database
		}
		items = append(items, &item)
	}
	return items, pgpkg.MapErr(rows.Err())
}
