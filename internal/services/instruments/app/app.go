package app

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	chdb "github.com/Mar1eena/TrB_V3/internal/pkg/db/clickhouse"
	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
	"github.com/Mar1eena/TrB_V3/internal/pkg/grpcx"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/Mar1eena/TrB_V3/internal/pkg/wait"
	"github.com/Mar1eena/TrB_V3/internal/services/instruments/server"
	tinvest "github.com/Mar1eena/trb_proto/gen/go/api/tinvest"
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
		ch         driver.Conn
		investConn *grpc.ClientConn
	)
	defer func() {
		if investConn != nil {
			if err := investConn.Close(); err != nil {
				l.Error().Err(err).Msg("ошибка закрытия соединения с invest")
			}
		}
		if ch != nil {
			if err := ch.Close(); err != nil {
				l.Error().Err(err).Msg("ошибка закрытия соединения с ClickHouse")
			}
		}
	}()

	g := wait.NewGroup(ctx, l)
	chSlot := wait.Go(g, "ClickHouse", func(ctx context.Context) (driver.Conn, error) {
		return chdb.Connect(ctx, chdb.ClickHouse_config())
	})
	investSlot := wait.Go(g, "invest", func(ctx context.Context) (*grpc.ClientConn, error) {
		addr := env.Addr("INVEST_API_URL", "INVEST_API_URL_DOCKER")
		if addr == "" {
			return nil, errors.New("INVEST_API_URL не задан")
		}
		l.Info().Str("addr", addr).Msg("подключение к gRPC invest")
		return grpcx.DialInsecureWithLogger(addr, l)
	})
	if err := g.Wait(); err != nil {
		l.Info().Err(err).Msg("сервис остановлен до подключения к зависимостям")
		return
	}
	ch = chSlot.Get()
	investConn = investSlot.Get()

	if err := chdb.EnsureShtSchema(ctx, ch); err != nil {
		l.Fatal().Err(err).Msg("не удалось подготовить схему TrB.sht")
	}

	port := env.Get("PORT")
	if port == "" {
		port = "9091"
	}
	addr := "0.0.0.0:" + port
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		l.Fatal().Err(err).Msg("не удалось начать прослушивание " + addr)
	}
	l.Info().Str("addr", addr).Msg("instruments слушает gRPC")

	gs := grpc.NewServer(grpcx.ServerOptions(l)...)
	server.Register(gs, server.New(ch, tinvest.NewInstrumentsServiceClient(investConn), l))

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
