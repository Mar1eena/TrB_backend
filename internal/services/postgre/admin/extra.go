package admin

import (
	"context"
	"strings"

	pgpkg "github.com/Mar1eena/TrB_V3/internal/services/postgre/pkg"
	pgapi "github.com/Mar1eena/trb_proto/gen/go/postgresql"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (a *Admin) ListPartitions(ctx context.Context, req *pgapi.ListPartitionsRequest) (*pgapi.PartitionList, error) {
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
	rows, err := pool.Query(ctx, `
SELECT
	n.nspname,
	c.relname,
	coalesce(pg_catalog.pg_get_expr(c.relpartbound, c.oid), ''),
	GREATEST(c.reltuples, 0)::bigint,
	coalesce(pg_catalog.pg_total_relation_size(c.oid), 0)
FROM pg_catalog.pg_inherits inh
JOIN pg_catalog.pg_class c ON c.oid = inh.inhrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_class p ON p.oid = inh.inhparent
JOIN pg_catalog.pg_namespace pn ON pn.oid = p.relnamespace
WHERE pn.nspname = $1 AND p.relname = $2
ORDER BY n.nspname, c.relname`, schema, table)
	if err != nil {
		return nil, pgpkg.MapErr(err)
	}
	defer rows.Close()
	items := make([]*pgapi.TablePartition, 0)
	for rows.Next() {
		var (
			item       pgapi.TablePartition
			rowsN, sz  int64
		)
		if err := rows.Scan(&item.Schema, &item.Name, &item.Expression, &rowsN, &sz); err != nil {
			return nil, pgpkg.MapErr(err)
		}
		item.TotalRows = u64(rowsN)
		item.TotalBytes = u64(sz)
		items = append(items, &item)
	}
	if err := rows.Err(); err != nil {
		return nil, pgpkg.MapErr(err)
	}
	a.log.Info().Str("table", table).Int("count", len(items)).Msg("список партиций получен")
	return &pgapi.PartitionList{Items: items}, nil
}

func (a *Admin) DropPartition(ctx context.Context, req *pgapi.DropPartitionRequest) (*pgapi.Status, error) {
	sql, err := pgpkg.DropPartitionSQL(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.poolFor(ctx, req.GetDatabase())
	if err != nil {
		return nil, err
	}
	out, err := a.exec(ctx, pool, sql)
	if err != nil {
		a.log.Error().Err(err).Str("partition", req.GetName()).Msg("не удалось удалить партицию")
		return nil, err
	}
	a.log.Info().Str("partition", req.GetName()).Msg("партиция обработана")
	return out, nil
}

func (a *Admin) ListProcesses(ctx context.Context, req *pgapi.ListProcessesRequest) (*pgapi.ProcessList, error) {
	if req == nil {
		req = &pgapi.ListProcessesRequest{}
	}
	q := `
SELECT
	pid,
	coalesce(usename, ''),
	coalesce(datname, ''),
	coalesce(application_name, ''),
	coalesce(client_addr::text, ''),
	coalesce(state, ''),
	coalesce(wait_event_type, ''),
	coalesce(wait_event, ''),
	coalesce(query, ''),
	backend_start,
	query_start,
	state_change,
	coalesce(backend_type, '')
FROM pg_catalog.pg_stat_activity
WHERE pid <> pg_backend_pid()`
	args := make([]any, 0, 1)
	if db := strings.TrimSpace(req.GetDatabase()); db != "" {
		id, err := pgpkg.Ident(db, "database")
		if err != nil {
			return nil, err
		}
		q += ` AND datname = $1`
		args = append(args, id)
	}
	if req.GetActiveOnly() {
		q += ` AND state = 'active'`
	}
	q += ` ORDER BY query_start NULLS LAST`
	rows, err := a.home.Query(ctx, q, args...)
	if err != nil {
		return nil, pgpkg.MapErr(err)
	}
	defer rows.Close()
	items := make([]*pgapi.ProcessInfo, 0)
	for rows.Next() {
		var (
			item                         pgapi.ProcessInfo
			backendStart, queryStart, st pgtype.Timestamptz
		)
		if err := rows.Scan(
			&item.Pid, &item.User, &item.Database, &item.ApplicationName, &item.ClientAddr,
			&item.State, &item.WaitEventType, &item.WaitEvent, &item.Query,
			&backendStart, &queryStart, &st, &item.BackendType,
		); err != nil {
			return nil, pgpkg.MapErr(err)
		}
		item.BackendStart = ts(backendStart)
		item.QueryStart = ts(queryStart)
		item.StateChange = ts(st)
		items = append(items, &item)
	}
	if err := rows.Err(); err != nil {
		return nil, pgpkg.MapErr(err)
	}
	a.log.Info().Int("count", len(items)).Msg("список процессов PostgreSQL получен")
	return &pgapi.ProcessList{Items: items}, nil
}

func (a *Admin) KillProcess(ctx context.Context, req *pgapi.KillProcessRequest) (*pgapi.Status, error) {
	if req == nil || req.GetPid() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "pid обязателен")
	}
	fn := "pg_cancel_backend"
	if req.GetTerminate() {
		fn = "pg_terminate_backend"
	}
	var ok bool
	if err := a.home.QueryRow(ctx, `SELECT `+fn+`($1)`, req.GetPid()).Scan(&ok); err != nil {
		a.log.Error().Err(err).Int32("pid", req.GetPid()).Msg("не удалось завершить процесс")
		return nil, pgpkg.MapErr(err)
	}
	if !ok {
		return nil, status.Errorf(codes.NotFound, "процесс %d не найден или не принял сигнал", req.GetPid())
	}
	a.log.Info().Int32("pid", req.GetPid()).Bool("terminate", req.GetTerminate()).Msg("сигнал процессу отправлен")
	return okStatus(), nil
}

func (a *Admin) ListLocks(ctx context.Context, req *pgapi.ListLocksRequest) (*pgapi.LockList, error) {
	if req == nil {
		req = &pgapi.ListLocksRequest{}
	}
	q := `
SELECT
	l.pid,
	coalesce(l.locktype, ''),
	coalesce(d.datname, ''),
	coalesce(n.nspname || '.' || c.relname, ''),
	coalesce(l.mode, ''),
	l.granted,
	l.fastpath,
	coalesce(a.query, '')
FROM pg_catalog.pg_locks l
LEFT JOIN pg_catalog.pg_database d ON d.oid = l.database
LEFT JOIN pg_catalog.pg_class c ON c.oid = l.relation
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_stat_activity a ON a.pid = l.pid
WHERE true`
	args := make([]any, 0, 1)
	if db := strings.TrimSpace(req.GetDatabase()); db != "" {
		id, err := pgpkg.Ident(db, "database")
		if err != nil {
			return nil, err
		}
		q += ` AND d.datname = $1`
		args = append(args, id)
	}
	if req.GetGrantedOnly() {
		q += ` AND l.granted`
	}
	q += ` ORDER BY l.pid`
	rows, err := a.home.Query(ctx, q, args...)
	if err != nil {
		return nil, pgpkg.MapErr(err)
	}
	defer rows.Close()
	items := make([]*pgapi.LockInfo, 0)
	for rows.Next() {
		var item pgapi.LockInfo
		if err := rows.Scan(
			&item.Pid, &item.Locktype, &item.Database, &item.Relation,
			&item.Mode, &item.Granted, &item.Fastpath, &item.Query,
		); err != nil {
			return nil, pgpkg.MapErr(err)
		}
		items = append(items, &item)
	}
	if err := rows.Err(); err != nil {
		return nil, pgpkg.MapErr(err)
	}
	a.log.Info().Int("count", len(items)).Msg("список блокировок получен")
	return &pgapi.LockList{Items: items}, nil
}

func (a *Admin) ListTablespaces(ctx context.Context, _ *pgapi.ListTablespacesRequest) (*pgapi.TablespaceList, error) {
	rows, err := a.home.Query(ctx, `
SELECT
	spcname,
	pg_catalog.pg_get_userbyid(spcowner),
	coalesce(pg_catalog.pg_tablespace_location(oid), ''),
	coalesce(pg_catalog.pg_tablespace_size(oid), 0)
FROM pg_catalog.pg_tablespace
ORDER BY spcname`)
	if err != nil {
		return nil, pgpkg.MapErr(err)
	}
	defer rows.Close()
	items := make([]*pgapi.TablespaceInfo, 0)
	for rows.Next() {
		var (
			item pgapi.TablespaceInfo
			size int64
		)
		if err := rows.Scan(&item.Name, &item.Owner, &item.Location, &size); err != nil {
			return nil, pgpkg.MapErr(err)
		}
		item.SizeBytes = u64(size)
		items = append(items, &item)
	}
	if err := rows.Err(); err != nil {
		return nil, pgpkg.MapErr(err)
	}
	a.log.Info().Int("count", len(items)).Msg("список tablespace получен")
	return &pgapi.TablespaceList{Items: items}, nil
}

func (a *Admin) GetMetrics(ctx context.Context, req *pgapi.GetMetricsRequest) (*pgapi.MetricsResponse, error) {
	if req == nil {
		req = &pgapi.GetMetricsRequest{}
	}
	q := `
SELECT
	coalesce(sum(numbackends), 0),
	coalesce(sum(xact_commit), 0),
	coalesce(sum(xact_rollback), 0),
	coalesce(sum(blks_read), 0),
	coalesce(sum(blks_hit), 0),
	coalesce(sum(tup_returned), 0),
	coalesce(sum(tup_fetched), 0),
	coalesce(sum(tup_inserted), 0),
	coalesce(sum(tup_updated), 0),
	coalesce(sum(tup_deleted), 0),
	coalesce(sum(conflicts), 0),
	coalesce(sum(temp_files), 0),
	coalesce(sum(temp_bytes), 0),
	coalesce(sum(deadlocks), 0)
FROM pg_catalog.pg_stat_database WHERE datname IS NOT NULL`
	args := make([]any, 0, 1)
	if db := strings.TrimSpace(req.GetDatabase()); db != "" {
		id, err := pgpkg.Ident(db, "database")
		if err != nil {
			return nil, err
		}
		q += ` AND datname = $1`
		args = append(args, id)
	}
	var backends, commits, rollbacks, reads, hits, ret, fet, ins, upd, del, conflicts, tmpFiles, tmpBytes, deadlocks float64
	if err := a.home.QueryRow(ctx, q, args...).Scan(
		&backends, &commits, &rollbacks, &reads, &hits, &ret, &fet, &ins, &upd, &del, &conflicts, &tmpFiles, &tmpBytes, &deadlocks,
	); err != nil {
		return nil, pgpkg.MapErr(err)
	}
	hitRatio := 0.0
	if hits+reads > 0 {
		hitRatio = hits / (hits + reads)
	}
	items := []*pgapi.MetricItem{
		{Name: "numbackends", Value: backends, Description: "активные подключения"},
		{Name: "xact_commit", Value: commits, Description: "зафиксированные транзакции"},
		{Name: "xact_rollback", Value: rollbacks, Description: "откаты транзакций"},
		{Name: "blks_read", Value: reads, Description: "блоки, прочитанные с диска"},
		{Name: "blks_hit", Value: hits, Description: "блоки из кэша"},
		{Name: "cache_hit_ratio", Value: hitRatio, Description: "доля попаданий в кэш"},
		{Name: "tup_returned", Value: ret, Description: "строки, возвращённые seq scan"},
		{Name: "tup_fetched", Value: fet, Description: "строки, выбранные по индексу"},
		{Name: "tup_inserted", Value: ins, Description: "вставленные строки"},
		{Name: "tup_updated", Value: upd, Description: "обновлённые строки"},
		{Name: "tup_deleted", Value: del, Description: "удалённые строки"},
		{Name: "conflicts", Value: conflicts, Description: "конфликты репликации"},
		{Name: "temp_files", Value: tmpFiles, Description: "временные файлы"},
		{Name: "temp_bytes", Value: tmpBytes, Description: "байты во временных файлах"},
		{Name: "deadlocks", Value: deadlocks, Description: "взаимоблокировки"},
	}
	a.log.Info().Int("metrics", len(items)).Msg("метрики PostgreSQL получены")
	return &pgapi.MetricsResponse{Metrics: items}, nil
}

func (a *Admin) GetTableOptions(ctx context.Context, _ *pgapi.TableOptionsRequest) (*pgapi.TableOptionsResponse, error) {
	types, err := a.selectNames(ctx, `
SELECT typname FROM pg_catalog.pg_type
WHERE typnamespace = 'pg_catalog'::regnamespace
  AND typtype = 'b' AND typelem = 0 AND typname NOT LIKE 'pg_%' AND typname NOT LIKE '\\_%'
ORDER BY typname`)
	if err != nil {
		a.log.Warn().Err(err).Msg("не удалось загрузить типы PostgreSQL")
	}
	methods, err := a.selectNames(ctx, `SELECT amname FROM pg_catalog.pg_am WHERE amtype = 'i' ORDER BY amname`)
	if err != nil {
		a.log.Warn().Err(err).Msg("не удалось загрузить методы индексов")
	}
	collations, err := a.selectNames(ctx, `
SELECT collname FROM pg_catalog.pg_collation
WHERE collname NOT LIKE 'pg_%'
ORDER BY collname`)
	if err != nil {
		a.log.Warn().Err(err).Msg("не удалось загрузить collation")
	}
	tablespaces, err := a.selectNames(ctx, `SELECT spcname FROM pg_catalog.pg_tablespace ORDER BY spcname`)
	if err != nil {
		a.log.Warn().Err(err).Msg("не удалось загрузить tablespace")
	}
	extras := []string{
		"integer", "bigint", "smallint", "numeric", "numeric(10,2)",
		"real", "double precision", "text", "varchar", "varchar(255)",
		"boolean", "date", "time", "timestamp", "timestamptz",
		"bytea", "uuid", "json", "jsonb", "inet",
	}
	seen := make(map[string]struct{}, len(types)+len(extras))
	outTypes := make([]string, 0, len(types)+len(extras))
	for _, t := range append(extras, types...) {
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		outTypes = append(outTypes, t)
	}
	return &pgapi.TableOptionsResponse{
		DataTypes:    outTypes,
		IndexMethods: methods,
		Collations:   collations,
		Tablespaces:  tablespaces,
	}, nil
}

func (a *Admin) selectNames(ctx context.Context, query string) ([]string, error) {
	rows, err := a.home.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name != "" {
			out = append(out, name)
		}
	}
	return out, rows.Err()
}
