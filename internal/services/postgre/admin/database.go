package admin

import (
	"context"
	"strings"

	pgpkg "github.com/Mar1eena/TrB_V3/internal/services/postgre/pkg"
	pgapi "github.com/Mar1eena/trb_proto/gen/go/postgresql"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const databaseSQL = `
SELECT
	d.datname,
	pg_catalog.pg_get_userbyid(d.datdba),
	pg_catalog.pg_encoding_to_char(d.encoding),
	d.datcollate,
	d.datctype,
	coalesce(pg_catalog.pg_database_size(d.datname), 0),
	d.datconnlimit,
	coalesce(s.numbackends, 0),
	d.datallowconn,
	t.spcname
FROM pg_catalog.pg_database d
JOIN pg_catalog.pg_tablespace t ON t.oid = d.dattablespace
LEFT JOIN pg_catalog.pg_stat_database s ON s.datid = d.oid`

func (a *Admin) CreateDatabase(ctx context.Context, req *pgapi.DatabaseSpec) (*pgapi.Status, error) {
	if req != nil && req.GetIfNotExists() {
		exists, err := a.databaseExists(ctx, req.GetName())
		if err != nil {
			return nil, err
		}
		if exists {
			return okStatus(), nil
		}
	}
	sql, err := pgpkg.CreateDatabaseSQL(req)
	if err != nil {
		return nil, err
	}
	pool, err := a.maint(ctx)
	if err != nil {
		return nil, err
	}
	out, err := a.exec(ctx, pool, sql)
	if err != nil {
		if req.GetIfNotExists() && status.Code(err) == codes.AlreadyExists {
			return okStatus(), nil
		}
		a.log.Error().Err(err).Str("name", req.GetName()).Msg("не удалось создать базу")
		return nil, err
	}
	a.log.Info().Str("name", req.GetName()).Msg("база создана")
	return out, nil
}

func (a *Admin) DropDatabase(ctx context.Context, req *pgapi.DatabaseName) (*pgapi.Status, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	if req.GetName() == a.homeDB {
		return nil, status.Error(codes.FailedPrecondition, "нельзя удалить базу, к которой подключён сервис")
	}
	sql, err := pgpkg.DropDatabaseSQL(req)
	if err != nil {
		return nil, err
	}
	a.closeExtra(req.GetName())
	pool, err := a.maint(ctx)
	if err != nil {
		return nil, err
	}
	out, err := a.exec(ctx, pool, sql)
	if err != nil {
		a.log.Error().Err(err).Str("name", req.GetName()).Msg("не удалось удалить базу")
		return nil, err
	}
	a.log.Info().Str("name", req.GetName()).Msg("база удалена")
	return out, nil
}

func (a *Admin) ListDatabases(ctx context.Context, req *pgapi.ListDatabasesRequest) (*pgapi.DatabaseList, error) {
	if req == nil {
		req = &pgapi.ListDatabasesRequest{}
	}
	q := databaseSQL
	var args []any
	if like := strings.TrimSpace(req.GetLike()); like != "" {
		q += ` WHERE d.datname LIKE $1`
		args = append(args, like)
	}
	q += ` ORDER BY d.datname`
	items, err := a.scanDatabases(ctx, a.home, q, args...)
	if err != nil {
		a.log.Error().Err(err).Msg("не удалось получить список баз")
		return nil, err
	}
	a.log.Info().Int("count", len(items)).Msg("список баз получен")
	return &pgapi.DatabaseList{Items: items}, nil
}

func (a *Admin) DatabaseInfo(ctx context.Context, req *pgapi.DatabaseName) (*pgapi.Database, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "тело запроса обязательно")
	}
	name, err := pgpkg.Ident(req.GetName(), "name")
	if err != nil {
		return nil, err
	}
	items, err := a.scanDatabases(ctx, a.home, databaseSQL+` WHERE d.datname = $1`, name)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, status.Errorf(codes.NotFound, "база %s не найдена", name)
	}
	a.log.Info().Str("name", name).Msg("информация о базе получена")
	return items[0], nil
}

func (a *Admin) databaseExists(ctx context.Context, name string) (bool, error) {
	id, err := pgpkg.Ident(name, "name")
	if err != nil {
		return false, err
	}
	var exists bool
	if err := a.home.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_database WHERE datname = $1)`, id).Scan(&exists); err != nil {
		return false, pgpkg.MapErr(err)
	}
	return exists, nil
}

func (a *Admin) scanDatabases(ctx context.Context, pool *pgxpool.Pool, query string, args ...any) ([]*pgapi.Database, error) {
	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, pgpkg.MapErr(err)
	}
	defer rows.Close()
	items := make([]*pgapi.Database, 0)
	for rows.Next() {
		var (
			item     pgapi.Database
			size     int64
			backends int32
		)
		if err := rows.Scan(
			&item.Name, &item.Owner, &item.Encoding, &item.Collation, &item.Ctype,
			&size, &item.ConnectionLimit, &backends, &item.AllowConnections, &item.Tablespace,
		); err != nil {
			return nil, pgpkg.MapErr(err)
		}
		item.SizeBytes = u64(size)
		item.NumBackends = uint32(backends)
		items = append(items, &item)
	}
	return items, pgpkg.MapErr(rows.Err())
}
