package admin

import (
	"context"
	"fmt"
	"strings"

	pgpkg "github.com/Mar1eena/TrB_V3/internal/services/postgre/pkg"
	pgapi "github.com/Mar1eena/trb_proto/gen/go/postgresql"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const tableSQL = `
SELECT
	current_database(),
	n.nspname,
	c.relname,
	CASE c.relkind
		WHEN 'r' THEN 'table'
		WHEN 'v' THEN 'view'
		WHEN 'm' THEN 'materialized_view'
		WHEN 'f' THEN 'foreign_table'
		WHEN 'p' THEN 'partitioned_table'
		ELSE c.relkind::text
	END,
	pg_catalog.pg_get_userbyid(c.relowner),
	GREATEST(c.reltuples, 0)::bigint,
	coalesce(pg_catalog.pg_total_relation_size(c.oid), 0),
	coalesce(pg_catalog.pg_indexes_size(c.oid), 0),
	CASE WHEN c.reltoastrelid <> 0 THEN coalesce(pg_catalog.pg_total_relation_size(c.reltoastrelid), 0) ELSE 0 END,
	coalesce(s.n_live_tup, 0),
	coalesce(s.n_dead_tup, 0),
	coalesce(pg_catalog.obj_description(c.oid, 'pg_class'), ''),
	coalesce(t.spcname, ''),
	CASE c.relpersistence WHEN 'u' THEN 'unlogged' WHEN 't' THEN 'temporary' ELSE 'permanent' END,
	s.last_vacuum,
	s.last_analyze,
	s.last_autovacuum,
	s.last_autoanalyze
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_stat_all_tables s ON s.relid = c.oid
LEFT JOIN pg_catalog.pg_tablespace t ON t.oid = nullif(c.reltablespace, 0)
WHERE c.relkind = ANY($1)`

func (a *Admin) CreateTable(ctx context.Context, req *pgapi.TableSpec) (*pgapi.Status, error) {
	stmts, err := pgpkg.CreateTableStatements(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	out, err := a.execAll(ctx, pool, stmts)
	if err != nil {
		a.log.Error().Err(err).Str("table", req.GetName()).Msg("не удалось создать таблицу")
		return nil, err
	}
	a.log.Info().Str("database", req.GetDatabase()).Str("schema", req.GetSchema()).Str("name", req.GetName()).Msg("таблица создана")
	return out, nil
}

func (a *Admin) DropTable(ctx context.Context, req *pgapi.TableName) (*pgapi.Status, error) {
	sql, err := pgpkg.DropTableSQL(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	out, err := a.exec(ctx, pool, sql)
	if err != nil {
		a.log.Error().Err(err).Str("table", req.GetName()).Msg("не удалось удалить таблицу")
		return nil, err
	}
	a.log.Info().Str("table", req.GetName()).Msg("таблица удалена")
	return out, nil
}

func (a *Admin) TruncateTable(ctx context.Context, req *pgapi.TableName) (*pgapi.Status, error) {
	sql, err := pgpkg.TruncateTableSQL(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	out, err := a.exec(ctx, pool, sql)
	if err != nil {
		if req.GetIfExists() && isNotFound(err) {
			return okStatus(), nil
		}
		a.log.Error().Err(err).Str("table", req.GetName()).Msg("не удалось очистить таблицу")
		return nil, err
	}
	a.log.Info().Str("table", req.GetName()).Msg("таблица очищена")
	return out, nil
}

func (a *Admin) RenameTable(ctx context.Context, req *pgapi.RenameTableRequest) (*pgapi.Status, error) {
	stmts, err := pgpkg.RenameTableStatements(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	out, err := a.execAll(ctx, pool, stmts)
	if err != nil {
		a.log.Error().Err(err).Str("table", req.GetName()).Msg("не удалось переименовать таблицу")
		return nil, err
	}
	a.log.Info().Str("table", req.GetName()).Str("new_name", req.GetNewName()).Msg("таблица переименована")
	return out, nil
}

func (a *Admin) VacuumTable(ctx context.Context, req *pgapi.VacuumTableRequest) (*pgapi.Status, error) {
	sql, err := pgpkg.VacuumTableSQL(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	out, err := a.exec(ctx, pool, sql)
	if err != nil {
		a.log.Error().Err(err).Str("table", req.GetName()).Msg("не удалось выполнить VACUUM")
		return nil, err
	}
	a.log.Info().Str("table", req.GetName()).Msg("VACUUM выполнен")
	return out, nil
}

func (a *Admin) AnalyzeTable(ctx context.Context, req *pgapi.AnalyzeTableRequest) (*pgapi.Status, error) {
	sql, err := pgpkg.AnalyzeTableSQL(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	out, err := a.exec(ctx, pool, sql)
	if err != nil {
		a.log.Error().Err(err).Str("table", req.GetName()).Msg("не удалось выполнить ANALYZE")
		return nil, err
	}
	a.log.Info().Str("table", req.GetName()).Msg("ANALYZE выполнен")
	return out, nil
}

func (a *Admin) ListTables(ctx context.Context, req *pgapi.ListTablesRequest) (*pgapi.TableList, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if err := requireDB(req.GetDatabase()); err != nil {
		return nil, err
	}
	kinds, err := tableKinds(req.GetKind())
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	q := tableSQL
	args := []any{kinds}
	n := 2
	if schema := strings.TrimSpace(req.GetSchema()); schema != "" {
		id, err := pgpkg.Ident(schema, "schema")
		if err != nil {
			return nil, err
		}
		q += fmt.Sprintf(` AND n.nspname = $%d`, n)
		args = append(args, id)
		n++
	} else {
		q += ` AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg_%'`
	}
	if like := strings.TrimSpace(req.GetLike()); like != "" {
		q += fmt.Sprintf(` AND c.relname LIKE $%d`, n)
		args = append(args, like)
	}
	q += ` ORDER BY n.nspname, c.relname`
	items, err := a.scanTables(ctx, pool, q, args...)
	if err != nil {
		a.log.Error().Err(err).Msg("не удалось получить список таблиц")
		return nil, err
	}
	a.log.Info().Str("database", req.GetDatabase()).Int("count", len(items)).Msg("список таблиц получен")
	return &pgapi.TableList{Items: items}, nil
}

func (a *Admin) TableInfo(ctx context.Context, req *pgapi.TableName) (*pgapi.Table, error) {
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
	name, err := pgpkg.Ident(req.GetName(), "name")
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	items, err := a.scanTables(ctx, pool, tableSQL+` AND n.nspname = $2 AND c.relname = $3`, []string{"r", "v", "m", "f", "p"}, schema, name)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, status.Errorf(codes.NotFound, "таблица %s.%s не найдена", schema, name)
	}
	tbl := items[0]
	cols, err := a.loadColumns(ctx, pool, schema, name)
	if err != nil {
		return nil, err
	}
	tbl.Columns = cols
	if tbl.GetKind() == "view" || tbl.GetKind() == "materialized_view" {
		def, err := a.loadViewDef(ctx, pool, schema, name, tbl.GetKind())
		if err != nil {
			a.log.Warn().Err(err).Str("schema", schema).Str("name", name).Msg("не удалось получить определение view")
			tbl.CreateTableQuery = reconstructCreate(tbl)
		} else {
			tbl.CreateTableQuery = def
		}
	} else {
		tbl.CreateTableQuery = reconstructCreate(tbl)
	}
	a.log.Info().Str("schema", schema).Str("name", name).Int("columns", len(cols)).Msg("информация о таблице получена")
	return tbl, nil
}

func (a *Admin) loadViewDef(ctx context.Context, pool *pgxpool.Pool, schema, name, kind string) (string, error) {
	var def string
	err := pool.QueryRow(ctx, `
SELECT pg_catalog.pg_get_viewdef(c.oid, true)
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2`, schema, name).Scan(&def)
	if err != nil {
		return "", pgpkg.MapErr(err)
	}
	kw := "VIEW"
	if kind == "materialized_view" {
		kw = "MATERIALIZED VIEW"
	}
	return "CREATE " + kw + " " + quoteCatalog(schema) + "." + quoteCatalog(name) + " AS\n" + def, nil
}

func (a *Admin) scanTables(ctx context.Context, pool *pgxpool.Pool, query string, args ...any) ([]*pgapi.Table, error) {
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, pgpkg.MapErr(err)
	}
	defer rows.Close()
	items := make([]*pgapi.Table, 0)
	for rows.Next() {
		var (
			item                                                       pgapi.Table
			rowsN, total, idx, toast, live, dead                       int64
			lastVac, lastAn, lastAutoVac, lastAutoAn                   pgtype.Timestamptz
		)
		if err := rows.Scan(
			&item.Database, &item.Schema, &item.Name, &item.Kind, &item.Owner,
			&rowsN, &total, &idx, &toast, &live, &dead,
			&item.Comment, &item.Tablespace, &item.Persistence,
			&lastVac, &lastAn, &lastAutoVac, &lastAutoAn,
		); err != nil {
			return nil, pgpkg.MapErr(err)
		}
		item.TotalRows = u64(rowsN)
		item.TotalBytes = u64(total)
		item.IndexBytes = u64(idx)
		item.ToastBytes = u64(toast)
		item.LiveTuples = u64(live)
		item.DeadTuples = u64(dead)
		item.LastVacuum = ts(lastVac)
		item.LastAnalyze = ts(lastAn)
		item.LastAutovacuum = ts(lastAutoVac)
		item.LastAutoanalyze = ts(lastAutoAn)
		items = append(items, &item)
	}
	return items, pgpkg.MapErr(rows.Err())
}

func (a *Admin) loadColumns(ctx context.Context, pool *pgxpool.Pool, schema, table string) ([]*pgapi.Column, error) {
	rows, err := pool.Query(ctx, `
SELECT
	a.attname,
	pg_catalog.format_type(a.atttypid, a.atttypmod),
	NOT a.attnotnull,
	coalesce(pg_catalog.pg_get_expr(ad.adbin, ad.adrelid), ''),
	a.attidentity <> '',
	CASE a.attidentity WHEN 'a' THEN 'ALWAYS' WHEN 'd' THEN 'BY DEFAULT' ELSE '' END,
	CASE a.attgenerated WHEN 's' THEN coalesce(pg_catalog.pg_get_expr(ad.adbin, ad.adrelid), '') ELSE '' END,
	coalesce(col.collname, ''),
	coalesce(pg_catalog.col_description(c.oid, a.attnum), ''),
	EXISTS (
		SELECT 1 FROM pg_catalog.pg_index i
		WHERE i.indrelid = a.attrelid AND i.indisprimary AND a.attnum = ANY (i.indkey)
	),
	EXISTS (
		SELECT 1 FROM pg_catalog.pg_index i
		WHERE i.indrelid = a.attrelid AND i.indisunique AND NOT i.indisprimary AND a.attnum = ANY (i.indkey)
	)
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
LEFT JOIN pg_catalog.pg_collation col ON col.oid = a.attcollation AND col.collname <> 'default'
WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum`, schema, table)
	if err != nil {
		return nil, pgpkg.MapErr(err)
	}
	defer rows.Close()
	items := make([]*pgapi.Column, 0)
	for rows.Next() {
		var col pgapi.Column
		if err := rows.Scan(
			&col.Name, &col.Type, &col.Nullable, &col.DefaultExpression,
			&col.IsIdentity, &col.IdentityGeneration, &col.GeneratedExpression,
			&col.Collation, &col.Comment, &col.PrimaryKey, &col.Unique,
		); err != nil {
			return nil, pgpkg.MapErr(err)
		}
		if col.IsIdentity || col.GeneratedExpression != "" {
			col.DefaultExpression = ""
		}
		items = append(items, &col)
	}
	return items, pgpkg.MapErr(rows.Err())
}

func tableKinds(kind string) ([]string, error) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "", "table":
		return []string{"r", "p"}, nil
	case "view":
		return []string{"v"}, nil
	case "materialized_view":
		return []string{"m"}, nil
	case "foreign_table":
		return []string{"f"}, nil
	case "partitioned_table":
		return []string{"p"}, nil
	default:
		return nil, status.Errorf(codes.InvalidArgument, "неизвестный kind: %s", kind)
	}
}

func reconstructCreate(t *pgapi.Table) string {
	if t.GetKind() == "view" || t.GetKind() == "materialized_view" {
		return t.GetCreateTableQuery()
	}
	if len(t.GetColumns()) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("CREATE TABLE ")
	b.WriteString(quoteCatalog(t.GetSchema()))
	b.WriteByte('.')
	b.WriteString(quoteCatalog(t.GetName()))
	b.WriteString(" (\n")
	for i, c := range t.GetColumns() {
		if i > 0 {
			b.WriteString(",\n")
		}
		b.WriteString("  ")
		b.WriteString(quoteCatalog(c.GetName()))
		b.WriteByte(' ')
		b.WriteString(c.GetType())
		if !c.GetNullable() {
			b.WriteString(" NOT NULL")
		}
	}
	b.WriteString("\n)")
	return b.String()
}

func quoteCatalog(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
