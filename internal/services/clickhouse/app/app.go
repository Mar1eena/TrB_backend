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
	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
	"github.com/Mar1eena/TrB_V3/internal/pkg/grpcx"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/Mar1eena/TrB_V3/internal/pkg/wait"
	"github.com/Mar1eena/TrB_V3/internal/services/clickhouse/server"
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

	var ch driver.Conn
	defer func() {
		if ch != nil {
			if err := ch.Close(); err != nil {
				l.Error().Err(err).Msg("ошибка закрытия соединения с ClickHouse")
			}
		}
	}()

	ch, err := wait.Until(ctx, l, "ClickHouse", func(ctx context.Context) (driver.Conn, error) {
		return clickhouse.Connect(ctx, clickhouse.ClickHouse_config())
	})
	if err != nil {
		l.Info().Err(err).Msg("сервис остановлен до подключения к зависимостям")
		return
	}

	if err := clickhouse.EnsureShtSchema(ctx, ch); err != nil {
		l.Fatal().Err(err).Msg("не удалось подготовить схему TrB.sht")
	}

	named := clickhouse.NamedConfigs()
	extras := make(map[string]driver.Conn)
	infos := make([]server.ConnInfo, 0, len(named))
	defaultName := clickhouse.DefaultConnectionName()
	for _, item := range named {
		infos = append(infos, server.ConnInfo{
			Name:     item.Name,
			Host:     item.Host,
			Dial:     item.Config.Addr,
			Database: item.Database,
			Default:  item.Default,
		})
		if item.Default {
			continue
		}
		conn, err := wait.Until(ctx, l, "ClickHouse:"+item.Name, func(ctx context.Context) (driver.Conn, error) {
			return clickhouse.Connect(ctx, item.Config)
		})
		if err != nil {
			l.Error().Err(err).Str("name", item.Name).Msg("не удалось подключить дополнительный ClickHouse")
			continue
		}
		extras[item.Name] = conn
		l.Info().Str("name", item.Name).Str("addr", item.Host).Msg("дополнительный ClickHouse подключён")
	}
	defer func() {
		for name, conn := range extras {
			if err := conn.Close(); err != nil {
				l.Error().Err(err).Str("name", name).Msg("ошибка закрытия дополнительного ClickHouse")
			}
		}
	}()

	port := env.Get("PORT")
	if !env.IsContainer() {
		// Envoy зеркалирует gRPC на host.docker.internal:50051.
		// PORT=9091 в .env — порт контейнеров, на хосте его не занимаем.
		if p := env.Get("DEBUG_PORT"); p != "" {
			port = p
		} else if port == "" || port == "9091" {
			port = "50051"
		}
	} else if port == "" {
		port = "9091"
	}

	// 0.0.0.0: Envoy ходит сюда через host.docker.internal (IPv4).
	// :port на Windows садится на [::] и не dual-stack.
	addr := "0.0.0.0:" + port
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		l.Fatal().Err(err).Msg("не удалось начать прослушивание " + addr)
	}
	l.Info().Str("addr", addr).Msg("clickhouse слушает gRPC")

	gs := grpc.NewServer(grpcx.ServerOptions(l)...)
	service := server.NewWithExtras(ch, l, extras, defaultName, infos...)
	server.Register(gs, service)

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
