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
	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/Mar1eena/TrB_V3/internal/pkg/wait"
	"github.com/Mar1eena/TrB_V3/internal/services/invest/server"
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

	var investClient *investgo.Client
	defer func() {
		if investClient != nil {
			if err := investClient.Stop(); err != nil {
				l.Error().Err(err).Msg("ошибка остановки клиента invest")
			} else {
				l.Info().Msg("клиент invest остановлен")
			}
		}
	}()
	investClient, err := wait.Until(ctx, l, "invest", func(ctx context.Context) (*investgo.Client, error) {
		return investgo.NewClient(ctx, investgo.LoadEnvConfig(), l)
	})
	if err != nil {
		l.Info().Err(err).Msg("сервис остановлен до подключения к зависимостям")
		return
	}

	port := env.Get("PORT")
	if port == "" {
		port = "9091"
	}

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		l.Fatal().Err(err).Msg("не удалось начать прослушивание порта " + port)
	}
	l.Info().Msg("прослушивание порта " + port)

	gs := grpc.NewServer(grpcx.ServerOptions(l)...)
	server.Register(gs, investClient, l)

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		<-gCtx.Done()
		gs.GracefulStop()
		return nil
	})

	g.Go(func() error {
		if err := gs.Serve(lis); err != nil {
			return err
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			l.Info().Msg("сервис корректно остановлен")
		} else {
			l.Error().Err(err).Msg("сервис остановлен с ошибкой")
		}
	} else {
		l.Info().Msg("сервис успешно остановлен")
	}
}
