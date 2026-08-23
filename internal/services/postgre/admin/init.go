package admin

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/Mar1eena/TrB_V3/internal/pkg/db/postgres"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	pgpkg "github.com/Mar1eena/TrB_V3/internal/services/postgre/pkg"
	pgapi "github.com/Mar1eena/trb_proto/gen/go/postgresql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Admin struct {
	pgapi.UnimplementedPostgreSQL_AdminServer
	home   *pgxpool.Pool
	homeDB string
	cfg    postgres.Config
	log    zlog.Logger

	mu    sync.Mutex
	extra map[string]*pgxpool.Pool
}

var _ pgapi.PostgreSQL_AdminServer = (*Admin)(nil)

func New(home *pgxpool.Pool, cfg postgres.Config, log zlog.Logger) *Admin {
	return &Admin{
		home:   home,
		homeDB: cfg.Database(),
		cfg:    cfg,
		log:    log,
		extra:  make(map[string]*pgxpool.Pool),
	}
}

func Register(srv *grpc.Server, service *Admin) {
	pgapi.RegisterPostgreSQL_AdminServer(srv, service)
}

func (a *Admin) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for name, p := range a.extra {
		p.Close()
		delete(a.extra, name)
	}
}

func (a *Admin) closeExtra(name string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if p, ok := a.extra[name]; ok {
		p.Close()
		delete(a.extra, name)
	}
}

func (a *Admin) poolFor(ctx context.Context, database string) (*pgxpool.Pool, error) {
	database = strings.TrimSpace(database)
	if database == "" || database == a.homeDB {
		return a.home, nil
	}
	if _, err := pgpkg.Ident(database, "database"); err != nil {
		return nil, err
	}
	a.mu.Lock()
	if p, ok := a.extra[database]; ok {
		a.mu.Unlock()
		return p, nil
	}
	a.mu.Unlock()

	p, err := postgres.Connect(ctx, a.cfg.WithDatabase(database).WithMaxConns(2))
	if err != nil {
		return nil, pgpkg.MapErr(err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if existing, ok := a.extra[database]; ok {
		p.Close()
		return existing, nil
	}
	a.extra[database] = p
	return p, nil
}

func (a *Admin) maint(ctx context.Context) (*pgxpool.Pool, error) {
	if a.homeDB == "postgres" || a.homeDB == "" {
		return a.home, nil
	}
	p, err := a.poolFor(ctx, "postgres")
	if err != nil {
		a.log.Warn().Err(err).Msg("нет доступа к базе postgres, используем основное подключение")
		return a.home, nil
	}
	return p, nil
}

func (a *Admin) simpleExec(ctx context.Context, pool *pgxpool.Pool, sql string) (pgconn.CommandTag, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	defer conn.Release()
	results, err := conn.Conn().PgConn().Exec(ctx, sql).ReadAll()
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	if len(results) == 0 {
		return pgconn.CommandTag{}, nil
	}
	return results[len(results)-1].CommandTag, nil
}

func (a *Admin) exec(ctx context.Context, pool *pgxpool.Pool, sql string) (*pgapi.Status, error) {
	if _, err := a.simpleExec(ctx, pool, sql); err != nil {
		return nil, pgpkg.MapErr(err)
	}
	return okStatus(), nil
}

func (a *Admin) execAll(ctx context.Context, pool *pgxpool.Pool, stmts []string) (*pgapi.Status, error) {
	for _, sql := range stmts {
		if _, err := a.exec(ctx, pool, sql); err != nil {
			return nil, err
		}
	}
	return okStatus(), nil
}

func okStatus() *pgapi.Status {
	return &pgapi.Status{Success: true, Message: "ok"}
}

func u64(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

func ts(t pgtype.Timestamptz) *timestamppb.Timestamp {
	if !t.Valid {
		return nil
	}
	return pgpkg.PbTime(t.Time)
}

func requireDB(name string) error {
	_, err := pgpkg.Ident(name, "database")
	return err
}

func isNotFound(err error) bool {
	return status.Code(err) == codes.NotFound
}

func elapsed(start time.Time) float64 {
	return time.Since(start).Seconds()
}
