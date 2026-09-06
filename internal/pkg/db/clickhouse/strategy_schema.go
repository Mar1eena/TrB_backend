package clickhouse

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// strategyDDL — таблицы домена стратегий (эквивалент configs/clickhouse/init.d/strategy.sql).
// Применяется на старте strategy-manage, т.к. init.d для clickhouse-db не смонтирован.
var strategyDDL = []string{
	`CREATE DATABASE IF NOT EXISTS TrB_strategy`,
	`CREATE TABLE IF NOT EXISTS TrB_strategy.equity_curve
	(
	    run_id UUID,
	    time DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
	    equity Float64 CODEC(ZSTD(1)),
	    cash Float64 CODEC(ZSTD(1)),
	    position_value Float64 CODEC(ZSTD(1)),
	    drawdown Float64 CODEC(ZSTD(1)),
	    ret Float64 CODEC(ZSTD(1)),
	    inserted_at DateTime64(3) DEFAULT now64(3)
	)
	ENGINE = ReplacingMergeTree(inserted_at)
	ORDER BY (run_id, time)
	SETTINGS index_granularity = 8192`,
	`CREATE TABLE IF NOT EXISTS TrB_strategy.trades
	(
	    run_id UUID,
	    trade_id UInt32,
	    is_long UInt8,
	    entry_time DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
	    entry_price Float64 CODEC(ZSTD(1)),
	    exit_time DateTime64(3) CODEC(DoubleDelta, ZSTD(1)),
	    exit_price Float64 CODEC(ZSTD(1)),
	    size Float64 CODEC(ZSTD(1)),
	    pnl Float64 CODEC(ZSTD(1)),
	    pnl_pct Float64 CODEC(ZSTD(1)),
	    bars_held UInt32,
	    mae Float64 CODEC(ZSTD(1)),
	    mfe Float64 CODEC(ZSTD(1)),
	    entry_reason LowCardinality(String),
	    exit_reason LowCardinality(String),
	    inserted_at DateTime64(3) DEFAULT now64(3)
	)
	ENGINE = ReplacingMergeTree(inserted_at)
	ORDER BY (run_id, trade_id)
	SETTINGS index_granularity = 8192`,
	`CREATE TABLE IF NOT EXISTS TrB_strategy.search_evals
	(
	    search_run_id UUID,
	    candidate_id UUID,
	    generation UInt16,
	    score Float64,
	    metrics Map(LowCardinality(String), Float64),
	    evaluated_at DateTime64(3) DEFAULT now64(3)
	)
	ENGINE = MergeTree
	ORDER BY (search_run_id, evaluated_at)
	TTL toDateTime(evaluated_at) + toIntervalDay(90)
	SETTINGS index_granularity = 8192`,
}

// EnsureStrategySchema создаёт БД/таблицы TrB_strategy, если их ещё нет.
func EnsureStrategySchema(ctx context.Context, conn driver.Conn) error {
	for _, stmt := range strategyDDL {
		if err := conn.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}
