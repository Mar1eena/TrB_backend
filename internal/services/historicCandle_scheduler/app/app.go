package app

import (
	"context"
	"errors"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	format_schemas "github.com/Mar1eena/TrB_V3/configs/clickhouse/format_schemas"
	trb_nats "github.com/Mar1eena/TrB_V3/internal/pkg/brokers/nats"
	"github.com/Mar1eena/TrB_V3/internal/pkg/db/clickhouse"
	"github.com/Mar1eena/TrB_V3/internal/pkg/db/postgres"
	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"github.com/Mar1eena/TrB_V3/internal/pkg/wait"
	orchpkg "github.com/Mar1eena/TrB_V3/internal/services/historicCandle_scheduler/pkg"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	pb "opensource.tbank.ru/invest/invest-go/proto"
)

// errSubjectOccupied — в стриме уже есть сообщение на этот subject.
// По инварианту historic_candle: один subject = одно задание.
var errSubjectOccupied = errors.New("задание на этот subject уже существует")

type config struct {
	subjectPrefix string
	tick          time.Duration
	minLag        time.Duration
	maxPerTick    int
	// fallbackInterval / fallbackUIDs used when Postgres has no enabled targets.
	fallbackInterval pb.CandleInterval
	fallbackUIDs     []string
}

type candidate struct {
	UID     string    `ch:"uid"`
	LagSec  int64     `ch:"lag_sec"`
	StartAt time.Time `ch:"start_time"`
}

func App() {
	if err := env.Load(); err != nil {
		panic(err)
	}
	l := zlog.New()
	cfg := configFromEnv()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var (
		conn   driver.Conn
		pgPool *pgxpool.Pool
		ncl    *trb_nats.Nats
	)
	defer func() {
		if conn != nil {
			if err := conn.Close(); err != nil {
				l.Error().Err(err).Msg("ошибка закрытия соединения с ClickHouse")
			}
		}
		if pgPool != nil {
			pgPool.Close()
		}
		if ncl != nil {
			ncl.C.Close()
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
		if err := postgres.EnsureSchema(ctx, pool); err != nil {
			pool.Close()
			return nil, err
		}
		return pool, nil
	})
	natsSlot := wait.Go(g, "NATS", func(ctx context.Context) (*trb_nats.Nats, error) {
		return trb_nats.NewNatsClient(ctx, trb_nats.Nats_config(), l)
	})
	if err := g.Wait(); err != nil {
		l.Info().Err(err).Msg("сервис остановлен до подключения к зависимостям")
		return
	}
	conn = chSlot.Get()
	pgPool = pgSlot.Get()
	ncl = natsSlot.Get()

	l.Info().
		Str("subject_prefix", cfg.subjectPrefix).
		Str("subject_pattern", cfg.subjectPrefix+".*.*").
		Dur("tick", cfg.tick).
		Dur("min_lag", cfg.minLag).
		Int("max_per_tick", cfg.maxPerTick).
		Str("fallback_interval", cfg.fallbackInterval.String()).
		Int("fallback_uids", len(cfg.fallbackUIDs)).
		Msg("оркестратор исторических свечей запущен")

	runLoop(ctx, conn, pgPool, ncl, l, cfg)
	l.Info().Msg("сервис успешно остановлен")
}

func runLoop(ctx context.Context, conn driver.Conn, pg *pgxpool.Pool, ncl *trb_nats.Nats, l zlog.Logger, cfg config) {
	ticker := time.NewTicker(cfg.tick)
	defer ticker.Stop()

	if err := tick(ctx, conn, pg, ncl, l, cfg); err != nil {
		l.Error().Err(err).Msg("ошибка тика оркестратора")
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := tick(ctx, conn, pg, ncl, l, cfg); err != nil {
				l.Error().Err(err).Msg("ошибка тика оркестратора")
			}
		}
	}
}

func tick(ctx context.Context, conn driver.Conn, pg *pgxpool.Pool, ncl *trb_nats.Nats, l zlog.Logger, cfg config) error {
	byInterval, err := loadTargetsByInterval(ctx, pg, cfg)
	if err != nil {
		return err
	}
	if len(byInterval) == 0 {
		l.Info().Msg("целей для догрузки нет")
		return nil
	}

	published := 0
	skipped := 0
	candidatesTotal := 0
	for interval, uids := range byInterval {
		candidates, err := selectCandidates(ctx, conn, interval, uids, cfg)
		if err != nil {
			return err
		}
		candidatesTotal += len(candidates)
		if len(candidates) > cfg.maxPerTick {
			candidates = candidates[:cfg.maxPerTick]
		}
		for _, c := range candidates {
			subject := orchpkg.TaskSubject(cfg.subjectPrefix, c.UID, int32(interval))
			if err := publishTask(ncl, subject, c.UID, int32(interval)); err != nil {
				if errors.Is(err, errSubjectOccupied) {
					skipped++
					continue
				}
				return err
			}
			published++
			l.Info().
				Str("subject", subject).
				Str("uid", c.UID).
				Int64("lag_sec", c.LagSec).
				Str("interval", interval.String()).
				Msg("опубликована задача догрузки")
		}
	}

	l.Info().
		Int("candidates", candidatesTotal).
		Int("published", published).
		Int("skipped", skipped).
		Int("intervals", len(byInterval)).
		Msg("тик оркестратора завершён")
	return nil
}

func loadTargetsByInterval(ctx context.Context, pg *pgxpool.Pool, cfg config) (map[pb.CandleInterval][]string, error) {
	targets, err := postgres.ListEnabledTargets(ctx, pg)
	if err != nil {
		return nil, err
	}

	out := make(map[pb.CandleInterval][]string)
	if len(targets) > 0 {
		for _, t := range targets {
			iv := pb.CandleInterval(t.Interval)
			out[iv] = append(out[iv], t.UID)
		}
		return out, nil
	}

	// Fallback: env whitelist / all instruments for a single interval.
	out[cfg.fallbackInterval] = append([]string(nil), cfg.fallbackUIDs...)
	return out, nil
}

func selectCandidates(ctx context.Context, conn driver.Conn, interval pb.CandleInterval, whitelist []string, cfg config) ([]candidate, error) {
	col := orchpkg.FirstCandleColumn(interval)
	minLagSec := int64(cfg.minLag.Seconds())
	if minLagSec <= 0 {
		minLagSec = 60
	}

	args := []any{int32(interval), minLagSec}
	uidFilter := "true"
	if len(whitelist) > 0 {
		uidFilter = "sht.uid IN ($3)"
		args = append(args, whitelist)
	}

	query := `
SELECT
	sht.uid AS uid,
	greatest(maxMerge(hctlda.max_time), sht.` + col + `) AS start_time,
	dateDiff('second', greatest(maxMerge(hctlda.max_time), sht.` + col + `), now64()) AS lag_sec
FROM TrB.sht sht FINAL
LEFT JOIN TrB.hct_last_download_agg hctlda FINAL
	ON sht.uid = hctlda.uid AND hctlda.interval = $1
WHERE sht.` + col + ` > 0
	AND ` + uidFilter + `
GROUP BY sht.uid, sht.` + col + `
HAVING lag_sec > $2
ORDER BY lag_sec DESC
LIMIT 1000`

	var out []candidate
	if err := conn.Select(ctx, &out, query, args...); err != nil {
		return nil, err
	}
	return out, nil
}

func publishTask(ncl *trb_nats.Nats, subject, uid string, interval int32) error {
	task := &format_schemas.HistoricCandleLoadTask{
		Uid:      []string{uid},
		Interval: interval,
	}
	data, err := proto.Marshal(task)
	if err != nil {
		return err
	}
	// ExpectLastSequencePerSubject(0) — публикуем только если на subject ещё нет сообщений.
	_, err = ncl.Jsc.Publish(subject, data, nats.ExpectLastSequencePerSubject(0))
	if orchpkg.IsSubjectOccupied(err) {
		return errSubjectOccupied
	}
	return err
}

func configFromEnv() config {
	prefix := env.Get("HCT_ORCH_SUBJECT_PREFIX")
	if prefix == "" {
		prefix = "TrB.HistoricCandle.Task"
	}

	tickSec := envInt("HCT_ORCH_TICK_SEC", 5)
	minLagSec := envInt("HCT_ORCH_MIN_LAG_SEC", 60)
	maxPerTick := envInt("HCT_ORCH_MAX_PER_TICK", 100)
	intervalNum := envInt("HCT_ORCH_INTERVAL", int(pb.CandleInterval_CANDLE_INTERVAL_1_MIN))

	return config{
		subjectPrefix:    prefix,
		tick:             time.Duration(tickSec) * time.Second,
		minLag:           time.Duration(minLagSec) * time.Second,
		maxPerTick:       maxPerTick,
		fallbackInterval: pb.CandleInterval(intervalNum),
		fallbackUIDs:     orchpkg.SplitCSV(env.Get("HCT_ORCH_UIDS")),
	}
}

func envInt(key string, def int) int {
	raw := env.Get(key)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	return n
}
