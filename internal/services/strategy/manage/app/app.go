package app

import (
	"context"
	"errors"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	trb_nats "github.com/Mar1eena/TrB_V3/internal/pkg/brokers/nats"
	"github.com/Mar1eena/TrB_V3/internal/pkg/db/clickhouse"
	"github.com/Mar1eena/TrB_V3/internal/pkg/db/postgres"
	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
	"github.com/Mar1eena/TrB_V3/internal/pkg/grpcx"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/Mar1eena/TrB_V3/internal/pkg/wait"
	"github.com/Mar1eena/TrB_V3/internal/services/strategy/manage/pkg/tasks"
	"github.com/Mar1eena/TrB_V3/internal/services/strategy/manage/server"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
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
		ch driver.Conn
		pg *pgxpool.Pool
		js *trb_nats.Nats
	)
	defer func() {
		if ch != nil {
			_ = ch.Close()
		}
		if pg != nil {
			pg.Close()
		}
		if js != nil && js.C != nil {
			_ = js.C.Drain()
		}
	}()

	g := wait.NewGroup(ctx, l)
	chSlot := wait.Go(g, "ClickHouse", func(ctx context.Context) (driver.Conn, error) {
		return clickhouse.Connect(ctx, clickhouse.ClickHouse_config())
	})
	pgSlot := wait.Go(g, "PostgreSQL", func(ctx context.Context) (*pgxpool.Pool, error) {
		pool, err := postgres.Connect(ctx, postgres.ConfigFromEnv())
		if err != nil {
			return nil, err
		}
		if err := postgres.EnsureStrategySchema(ctx, pool); err != nil {
			pool.Close()
			return nil, err
		}
		return pool, nil
	})
	jsSlot := wait.Go(g, "NATS", func(ctx context.Context) (*trb_nats.Nats, error) {
		return trb_nats.NewNatsClient(ctx, trb_nats.Nats_config(), l)
	})
	if err := g.Wait(); err != nil {
		l.Info().Err(err).Msg("сервис остановлен до подключения к зависимостям")
		return
	}
	ch, pg, js = chSlot.Get(), pgSlot.Get(), jsSlot.Get()

	if err := clickhouse.EnsureStrategySchema(ctx, ch); err != nil {
		l.Fatal().Err(err).Msg("не удалось подготовить схему TrB_strategy в ClickHouse")
	}
	if err := ensureStream(js); err != nil {
		l.Fatal().Err(err).Msg("не удалось подготовить NATS стрим/консьюмеры")
	}

	port := env.First("STRATEGY_PORT", "PORT")
	if port == "" {
		port = "9094"
	}
	addr := "0.0.0.0:" + port
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		l.Fatal().Err(err).Msg("не удалось начать прослушивание " + addr)
	}
	l.Info().Str("addr", addr).Msg("strategy-manage слушает gRPC")

	gs := grpc.NewServer(grpcx.ServerOptions(l)...)
	server.Register(gs, server.New(pg, ch, js, l))

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		<-egCtx.Done()
		gs.GracefulStop()
		return nil
	})
	eg.Go(func() error {
		return gs.Serve(lis)
	})

	if err := eg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		l.Error().Err(err).Msg("сервис остановлен с ошибкой")
		return
	}
	l.Info().Msg("сервис успешно остановлен")
}

// ensureStream идемпотентно создаёт WorkQueue-стримы strategy_tasks (бэктест + поиск)
// и strategy_eval (fan-out оценки кандидатов) с их durable-консьюмерами.
func ensureStream(js *trb_nats.Nats) error {
	streams := []struct {
		cfg       *nats.StreamConfig
		consumers []*nats.ConsumerConfig
	}{
		{
			cfg: &nats.StreamConfig{
				Name:        tasks.StreamStrategyTasks,
				Description: "Задачи бэктеста и генетического поиска стратегий (proto BacktestTask/SearchTask).",
				Subjects:    []string{tasks.SubjBacktestTasks, tasks.SubjSearchTasks},
				Retention:   nats.WorkQueuePolicy,
				Storage:     nats.FileStorage,
				Discard:     nats.DiscardOld,
				Duplicates:  2 * time.Minute,
			},
			consumers: []*nats.ConsumerConfig{
				{
					Durable:       tasks.ConsumerBacktest,
					FilterSubject: tasks.SubjBacktestTasks,
					AckPolicy:     nats.AckExplicitPolicy,
					MaxDeliver:    3,
					MaxAckPending: 4,
					AckWait:       15 * time.Minute,
					DeliverPolicy: nats.DeliverAllPolicy,
				},
				{
					Durable:       tasks.ConsumerSearch,
					FilterSubject: tasks.SubjSearchTasks,
					AckPolicy:     nats.AckExplicitPolicy,
					MaxDeliver:    2,
					MaxAckPending: 1,
					AckWait:       30 * time.Minute,
					DeliverPolicy: nats.DeliverAllPolicy,
				},
			},
		},
		{
			cfg: &nats.StreamConfig{
				Name:        tasks.StreamStrategyEval,
				Description: "Оценка одного кандидата генетического поиска (proto EvalTask). Ответы — core-NATS.",
				Subjects:    []string{tasks.SubjEvalTasks},
				Retention:   nats.WorkQueuePolicy,
				Storage:     nats.FileStorage,
				Discard:     nats.DiscardOld,
				Duplicates:  5 * time.Minute,
			},
			consumers: []*nats.ConsumerConfig{
				{
					Durable:       tasks.ConsumerEvalWorker,
					FilterSubject: tasks.SubjEvalTasks,
					AckPolicy:     nats.AckExplicitPolicy,
					MaxDeliver:    3,
					MaxAckPending: 256, // shared всеми репликами strategy-eval-worker
					AckWait:       5 * time.Minute,
					DeliverPolicy: nats.DeliverAllPolicy,
				},
			},
		},
	}

	for _, s := range streams {
		if _, err := js.Jsc.AddStream(s.cfg); err != nil && !isExists(err) {
			if _, uerr := js.Jsc.UpdateStream(s.cfg); uerr != nil {
				return err
			}
		}
		for _, c := range s.consumers {
			if _, err := js.Jsc.AddConsumer(s.cfg.Name, c); err != nil && !isExists(err) {
				return err
			}
		}
	}
	return nil
}

func isExists(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already in use") || strings.Contains(msg, "already exists") || errors.Is(err, nats.ErrStreamNameAlreadyInUse) || errors.Is(err, nats.ErrConsumerNameAlreadyInUse)
}
