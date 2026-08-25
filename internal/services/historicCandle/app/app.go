// Package app — воркер догрузки исторических свечей.
//
// Сервис непрерывно выбирает включённые цели из hct_scheduler_target
// (через ClickHouse postgresql()) и догружает свечи через gRPC-сервис invest.
package app

import (
	"context"
	"errors"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Mar1eena/TrB_V3/internal/pkg/db/clickhouse"
	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
	"github.com/Mar1eena/TrB_V3/internal/pkg/grpcx"
	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/Mar1eena/TrB_V3/internal/pkg/wait"
	"google.golang.org/grpc"
)

const defaultMinUpdate = 60.0

type config struct {
	minUpdate float64
}

func App() {
	if err := env.Load(); err != nil {
		panic(err)
	}
	cfg := configFromEnv()
	l := zlog.New()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var (
		investConn *grpc.ClientConn
		conn       driver.Conn
	)
	defer func() {
		if investConn != nil {
			if err := investConn.Close(); err != nil {
				l.Error().Err(err).Msg("ошибка закрытия соединения с invest")
			} else {
				l.Info().Msg("соединение с invest закрыто")
			}
		}
		if conn != nil {
			if err := conn.Close(); err != nil {
				l.Error().Err(err).Msg("ошибка закрытия соединения с ClickHouse")
			} else {
				l.Info().Msg("соединение с ClickHouse закрыто")
			}
		}
	}()

	g := wait.NewGroup(ctx, l)
	investSlot := wait.Go(g, "invest", func(ctx context.Context) (*grpc.ClientConn, error) {
		addr, err := investAPIAddr()
		if err != nil {
			return nil, err
		}
		l.Info().Str("addr", addr).Msg("подключение к gRPC invest")
		return grpcx.DialInsecureWithLogger(addr, l)
	})
	chSlot := wait.Go(g, "ClickHouse", func(ctx context.Context) (driver.Conn, error) {
		return clickhouse.Connect(ctx, clickhouse.ClickHouse_config())
	})
	if err := g.Wait(); err != nil {
		l.Info().Err(err).Msg("сервис остановлен до подключения к зависимостям")
		return
	}
	investConn = investSlot.Get()
	conn = chSlot.Get()

	md := investgo.NewMarketDataClient(ctx, investConn, l)
	if err := runWorker(ctx, md, conn, l, cfg); err != nil && !errors.Is(err, context.Canceled) {
		l.Error().Err(err).Msg("сервис остановлен с ошибкой")
		return
	}
	l.Info().Msg("сервис успешно остановлен")
}

func configFromEnv() config {
	return config{
		minUpdate: envFloat("INTERVAL_UPDATE_HC", defaultMinUpdate),
	}
}

func envFloat(key string, fallback float64) float64 {
	raw := env.Get(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return fallback
	}
	return float64(n)
}

func investAPIAddr() (string, error) {
	addr := env.Addr("INVEST_API_URL", "INVEST_API_URL_DOCKER")
	if addr == "" {
		return "", errors.New("INVEST_API_URL не задан")
	}
	return addr, nil
}
