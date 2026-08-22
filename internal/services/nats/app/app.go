package app

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"syscall"

	trb_nats "github.com/Mar1eena/TrB_V3/internal/pkg/brokers/nats"
	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
	"github.com/Mar1eena/TrB_V3/internal/pkg/grpcx"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/Mar1eena/TrB_V3/internal/pkg/wait"
	"github.com/Mar1eena/TrB_V3/internal/services/nats/server"
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

	var natsClient *trb_nats.Nats
	defer func() {
		if natsClient != nil {
			natsClient.C.Close()
			l.Info().Msg("отключение от NATS")
		}
	}()
	natsClient, err := wait.Until(ctx, l, "NATS", func(ctx context.Context) (*trb_nats.Nats, error) {
		return trb_nats.NewNatsClient(ctx, trb_nats.Nats_config(), l)
	})
	if err != nil {
		l.Info().Err(err).Msg("сервис остановлен до подключения к зависимостям")
		return
	}

	// gRPC server
	lis, err := net.Listen("tcp", ":"+env.Get("PORT"))
	if err != nil {
		l.Fatal().Err(err).Msg("не удалось начать прослушивание порта " + env.Get("PORT"))
	} else {
		l.Info().Msg("прослушивание порта " + env.Get("PORT"))
	}

	gs := grpc.NewServer(grpcx.ServerOptions(l)...)
	service := server.NewNatsService(natsClient)
	server.RegisterNats_AdminServer(gs, service)

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
