package app

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
	"github.com/Mar1eena/TrB_V3/internal/pkg/grpcx"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/Mar1eena/TrB_V3/internal/pkg/wait"
	chclient "github.com/Mar1eena/TrB_V3/internal/services/api/clickhouse/client"
	investclient "github.com/Mar1eena/TrB_V3/internal/services/api/invest/client"
	"github.com/Mar1eena/TrB_V3/internal/services/test/server"
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
		investConn *grpc.ClientConn
		chConn     *grpc.ClientConn
	)
	defer func() {
		if investConn != nil {
			if err := investConn.Close(); err != nil {
				l.Error().Err(err).Msg("ошибка закрытия соединения с invest")
			}
		}
		if chConn != nil {
			if err := chConn.Close(); err != nil {
				l.Error().Err(err).Msg("ошибка закрытия соединения с clickhouse")
			}
		}
	}()

	g := wait.NewGroup(ctx, l)
	investSlot := wait.Go(g, "invest", func(ctx context.Context) (*grpc.ClientConn, error) {
		return investclient.DialFromEnv()
	})
	chSlot := wait.Go(g, "clickhouse", func(ctx context.Context) (*grpc.ClientConn, error) {
		return chclient.DialFromEnv()
	})
	if err := g.Wait(); err != nil {
		l.Info().Err(err).Msg("сервис остановлен до подключения к зависимостям")
		return
	}
	investConn = investSlot.Get()
	chConn = chSlot.Get()

	port := env.Get("PORT")
	if port == "" {
		port = "9091"
	}
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		l.Fatal().Err(err).Msg("не удалось начать прослушивание порта " + port)
	}
	l.Info().Str("port", port).Msg("test слушает gRPC")

	gs := grpc.NewServer(grpcx.ServerOptions(l)...)
	service := server.New(
		investclient.NewInstruments(investConn),
		chclient.New(chConn),
		l,
	)
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
