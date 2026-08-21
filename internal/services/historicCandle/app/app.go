// Package app — воркер догрузки исторических свечей.
//
// Сервис принимает задания исключительно из JetStream-стрима historic_candle.
// Каждое задание живёт на отдельном subject вида:
//
//	TrB.HistoricCandle.Task.{uid}.{interval}
//
// В стриме на один такой subject может быть не больше одного сообщения
// (см. maxmsgspersubject: 1 и discardnewpersubject в configs/nats-server/streams.yaml).
// Пока сообщение не подтверждено ACK, повторная публикация в тот же subject отклоняется.
// После ACK слот освобождается, и планировщик может поставить новое задание.
package app

import (
	"context"
	"errors"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	trb_nats "github.com/Mar1eena/TrB_V3/internal/pkg/brokers/nats"
	"github.com/Mar1eena/TrB_V3/internal/pkg/db/clickhouse"
	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/Mar1eena/TrB_V3/internal/pkg/wait"
)

const (
	defaultSubject   = "TrB.HistoricCandle.Task.*.*"
	defaultConsumer  = "historic_candle_cons"
	defaultStream    = "historic_candle"
	defaultMinUpdate = 60.0
)

type config struct {
	stream    string
	subject   string
	consumer  string
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
		investClient *investgo.Client
		conn         driver.Conn
		natsClient   *trb_nats.Nats
	)
	defer func() {
		if investClient != nil {
			if err := investClient.Conn.Close(); err != nil {
				l.Error().Err(err).Msg("ошибка остановки клиента invest")
			} else {
				l.Info().Msg("клиент invest остановлен")
			}
		}
		if conn != nil {
			if err := conn.Close(); err != nil {
				l.Error().Err(err).Msg("ошибка закрытия соединения с ClickHouse")
			} else {
				l.Info().Msg("соединение с ClickHouse закрыто")
			}
		}
		if natsClient != nil {
			natsClient.C.Close()
			l.Info().Msg("отключение от NATS")
		}
	}()

	g := wait.NewGroup(ctx, l)
	investSlot := wait.Go(g, "invest", func(ctx context.Context) (*investgo.Client, error) {
		return investgo.NewClient(ctx, investgo.LoadEnvConfig(), l)
	})
	chSlot := wait.Go(g, "ClickHouse", func(ctx context.Context) (driver.Conn, error) {
		return clickhouse.Connect(ctx, clickhouse.ClickHouse_config())
	})
	natsSlot := wait.Go(g, "NATS", func(ctx context.Context) (*trb_nats.Nats, error) {
		return trb_nats.NewNatsClient(ctx, trb_nats.Nats_config(), l)
	})
	if err := g.Wait(); err != nil {
		l.Info().Err(err).Msg("сервис остановлен до подключения к зависимостям")
		return
	}
	investClient = investSlot.Get()
	conn = chSlot.Get()
	natsClient = natsSlot.Get()

	md := investClient.NewMarketDataServiceClient()
	if err := runWorker(ctx, natsClient, md, conn, l, cfg); err != nil && !errors.Is(err, context.Canceled) {
		l.Error().Err(err).Msg("сервис остановлен с ошибкой")
		return
	}
	l.Info().Msg("сервис успешно остановлен")
}

func configFromEnv() config {
	return config{
		stream:    envOr("HCT_NATS_STREAM", defaultStream),
		subject:   envOr("HCT_NATS_SUBJECT", defaultSubject),
		consumer:  envOr("HCT_NATS_CONSUMER", defaultConsumer),
		minUpdate: envFloat("INTERVAL_UPDATE_HC", defaultMinUpdate),
	}
}

func envOr(key, fallback string) string {
	if v := env.Get(key); v != "" {
		return v
	}
	return fallback
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
