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
	"github.com/Mar1eena/TrB_V3/internal/services/indicators/manage/server"
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

	port := env.First("INDICATORS_PORT", "PORT")
	if port == "" {
		port = "9093"
	}
	addr := "0.0.0.0:" + port
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		l.Fatal().Err(err).Msg("не удалось начать прослушивание " + addr)
	}
	l.Info().Str("addr", addr).Msg("indicators-manage (Indicator_Settings) слушает gRPC")

	gs := grpc.NewServer(grpcx.ServerOptions(l)...)
	server.Register(gs, ch, l)

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
			return
		}
		l.Error().Err(err).Msg("сервис остановлен с ошибкой")
		return
	}
	l.Info().Msg("сервис успешно остановлен")
}
