package app

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Mar1eena/TrB_V3/internal/pkg/db/clickhouse"
	"github.com/Mar1eena/TrB_V3/internal/pkg/db/postgres"
	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
	"github.com/Mar1eena/TrB_V3/internal/pkg/grpcx"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/Mar1eena/TrB_V3/internal/pkg/wait"
	"github.com/Mar1eena/TrB_V3/internal/services/postgre/admin"
	"github.com/Mar1eena/TrB_V3/internal/services/postgre/server"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

func App() {
	if err := env.Load(); err != nil {
		panic(err)
	}
	l := zlog.New()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var (
		ch  driver.Conn
		pg  *pgxpool.Pool
		adm *admin.Admin
	)
	defer func() {
		if adm != nil {
			adm.Close()
		}
		if ch != nil {
			if err := ch.Close(); err != nil {
				l.Error().Err(err).Msg("ошибка закрытия соединения с ClickHouse")
			}
		}
		if pg != nil {
			pg.Close()
		}
	}()

	g := wait.NewGroup(ctx, l)
	chSlot := wait.Go(g, "ClickHouse", func(ctx context.Context) (driver.Conn, error) {
		return clickhouse.Connect(ctx, clickhouse.ClickHouse_config())
	})
	pgCfg := postgres.ConfigFromEnv()
	pgSlot := wait.Go(g, "PostgreSQL", func(ctx context.Context) (*pgxpool.Pool, error) {
		pool, err := postgres.Connect(ctx, pgCfg)
		if err != nil {
			return nil, err
		}
		if err := postgres.EnsureSchema(ctx, pool); err != nil {
			pool.Close()
			return nil, err
		}
		return pool, nil
	})
	if err := g.Wait(); err != nil {
		l.Info().Err(err).Msg("сервис остановлен до подключения к зависимостям")
		return
	}
	ch = chSlot.Get()
	pg = pgSlot.Get()

	if err := clickhouse.EnsureShtSchema(ctx, ch); err != nil {
		l.Fatal().Err(err).Msg("не удалось подготовить схему TrB.sht")
	}

	var peers []admin.Peer
	for _, item := range postgres.NamedConfigs() {
		if item.Default {
			continue
		}
		pool, err := postgres.Connect(ctx, item.Config)
		if err != nil {
			l.Error().Err(err).Str("name", item.Name).Msg("не удалось подключить дополнительный PostgreSQL")
			continue
		}
		peers = append(peers, admin.Peer{Name: item.Name, Host: item.Host, Home: pool, Cfg: item.Config})
		l.Info().Str("name", item.Name).Str("host", item.Host).Msg("дополнительный PostgreSQL подключён")
	}

	port := env.Get("PORT")
	if port == "" {
		port = "9091"
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		l.Fatal().Err(err).Msg("не удалось начать прослушивание порта " + port)
	}
	l.Info().Str("port", port).Msg("data слушает gRPC")

	gs := grpc.NewServer(grpcx.ServerOptions(l)...)
	biz := server.New(ch, pg, l)
	adm = admin.NewWithPeers(pg, pgCfg, l, peers)
	server.Register(gs, biz)
	admin.Register(gs, adm)

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		<-egCtx.Done()
		gs.GracefulStop()
		return nil
	})
	eg.Go(func() error {
		return gs.Serve(lis)
	})

	if err := eg.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			l.Info().Msg("сервис корректно остановлен")
		} else {
			l.Error().Err(err).Msg("сервис остановлен с ошибкой")
		}
		return
	}
	l.Info().Msg("сервис успешно остановлен")
}
