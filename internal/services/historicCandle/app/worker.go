package app

import (
	"context"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Mar1eena/TrB_V3/internal/pkg/db/postgres"
	"github.com/Mar1eena/TrB_V3/internal/pkg/investgo"
	"github.com/Mar1eena/TrB_V3/internal/pkg/log/zlog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func runWorker(
	ctx context.Context,
	md *investgo.MarketDataServiceClient,
	conn driver.Conn,
	l zlog.Logger,
	cfg config,
) error {
	pgTable := postgres.ClickHouseTableExpr("hct_scheduler_target")
	l.Info().Msg("воркер исторических свечей опрашивает hct_scheduler_target")

	idleWait := time.Duration(cfg.minUpdate) * time.Second

	for {
		if ctx.Err() != nil {
			return nil
		}

		targets, err := investgo.SelectSchedulerTargets(ctx, conn, pgTable, 1, cfg.minUpdate)
		if err != nil {
			l.Error().Err(err).Msg("ошибка выборки целей")
			if !sleep(ctx, idleWait) {
				return nil
			}
			continue
		}
		if len(targets) == 0 {
			l.Info().Msg("нет целей с отставанием для догрузки")
			if !sleep(ctx, idleWait) {
				return nil
			}
			continue
		}

		target := targets[0]
		l.Info().
			Str("uid", target.Uid).
			Int32("interval", target.Interval).
			Time("start_time", target.StartTime).
			Msg("получена цель догрузки")

		err = processTarget(ctx, md, conn, l, target, cfg.minUpdate)
		if errors.Is(err, investgo.ErrIntervalUpdate) {
			if !sleep(ctx, idleWait) {
				return nil
			}
			continue
		}
		if isCanceledErr(err) || ctx.Err() != nil {
			return nil
		}
		if err != nil {
			if isUnavailableErr(err) {
				l.Warn().Err(err).Msg("gRPC invest недоступен, повтор после паузы")
			} else {
				l.Error().
					Err(err).
					Str("uid", target.Uid).
					Int32("interval", target.Interval).
					Msg("ошибка догрузки инструмента")
			}
			if !sleep(ctx, idleWait) {
				return nil
			}
			continue
		}
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		d = time.Second
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func isCanceledErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return true
	}
	return false
}

func isUnavailableErr(err error) bool {
	return status.Code(err) == codes.Unavailable
}

func processTarget(
	ctx context.Context,
	md *investgo.MarketDataServiceClient,
	conn driver.Conn,
	l zlog.Logger,
	target investgo.ShareData,
	minUpdate float64,
) error {
	err := target.Processing(ctx, md, conn, minUpdate)
	if err != nil {
		if errors.Is(err, investgo.ErrIntervalUpdate) {
			l.Warn().
				Str("uid", target.Uid).
				Int32("interval", target.Interval).
				Msg("отставание меньше INTERVAL_UPDATE_HC, догрузка приостановлена")
			return err
		}
		return err
	}

	l.Info().
		Str("uid", target.Uid).
		Int32("interval", target.Interval).
		Msg("окно успешно загружено")
	return nil
}
