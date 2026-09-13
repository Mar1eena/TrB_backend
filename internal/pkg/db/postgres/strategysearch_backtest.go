package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- strategysearch_backtest_run: отдельные (не связанные с Optuna-поиском) прогоны бэктеста ---

type BacktestRunRow struct {
	ID            string
	StrategyID    *string
	Spec          json.RawMessage
	SpecHash      int64
	UID           string
	Interval      int32
	PeriodStart   time.Time
	PeriodEnd     time.Time
	Config        json.RawMessage
	Status        string
	Error         string
	EngineVersion string
	DedupKey      *string
	CreatedAt     time.Time
	StartedAt     *time.Time
	FinishedAt    *time.Time
}

type NewBacktestRun struct {
	StrategyID  *string
	Spec        json.RawMessage
	SpecHash    int64
	UID         string
	Interval    int32
	PeriodStart time.Time
	PeriodEnd   time.Time
	Config      json.RawMessage
	DedupKey    *string
}

func InsertBacktestRun(ctx context.Context, pool *pgxpool.Pool, in NewBacktestRun) (BacktestRunRow, error) {
	var r BacktestRunRow
	err := pool.QueryRow(ctx, `
		INSERT INTO strategysearch_backtest_run
			(strategy_id, spec, spec_hash, uid, interval, period_start, period_end, config, dedup_key, status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'queued')
		RETURNING `+backtestRunCols,
		in.StrategyID, in.Spec, in.SpecHash, in.UID, in.Interval, in.PeriodStart, in.PeriodEnd, in.Config, in.DedupKey,
	).Scan(scanBacktestRun(&r)...)
	return r, err
}

func GetBacktestRun(ctx context.Context, pool *pgxpool.Pool, id string) (BacktestRunRow, error) {
	var r BacktestRunRow
	err := pool.QueryRow(ctx, `SELECT `+backtestRunCols+` FROM strategysearch_backtest_run WHERE id = $1`, id).Scan(scanBacktestRun(&r)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

// FindSucceededBacktestRun возвращает завершённый успешный прогон по dedup_key.
func FindSucceededBacktestRun(ctx context.Context, pool *pgxpool.Pool, dedupKey string) (BacktestRunRow, error) {
	var r BacktestRunRow
	err := pool.QueryRow(ctx, `
		SELECT `+backtestRunCols+`
		FROM strategysearch_backtest_run
		WHERE dedup_key = $1 AND status = 'succeeded'
		ORDER BY finished_at DESC NULLS LAST
		LIMIT 1`, dedupKey,
	).Scan(scanBacktestRun(&r)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

func CancelBacktestRun(ctx context.Context, pool *pgxpool.Pool, id string) (BacktestRunRow, error) {
	var r BacktestRunRow
	err := pool.QueryRow(ctx, `
		UPDATE strategysearch_backtest_run SET status = 'canceled', finished_at = now()
		WHERE id = $1 AND status IN ('queued','running')
		RETURNING `+backtestRunCols, id,
	).Scan(scanBacktestRun(&r)...)
	if errors.Is(err, pgx.ErrNoRows) {
		// либо нет строки, либо она уже в терминальном статусе — вернём текущее состояние
		return GetBacktestRun(ctx, pool, id)
	}
	return r, err
}

type BacktestRunFilter struct {
	StrategyID string
	UID        string
	Status     string
	SortBy     string
	SortDesc   bool
	Limit      int
	Offset     int
}

// ListBacktestRuns — прогоны + метрики (LEFT JOIN strategysearch_backtest_result).
func ListBacktestRuns(ctx context.Context, pool *pgxpool.Pool, f BacktestRunFilter) ([]BacktestRunRow, []json.RawMessage, int, error) {
	where := []string{"true"}
	args := []any{}
	add := func(cond string, val any) {
		args = append(args, val)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if f.StrategyID != "" {
		add("r.strategy_id = $%d", f.StrategyID)
	}
	if f.UID != "" {
		add("r.uid = $%d", f.UID)
	}
	if f.Status != "" {
		add("r.status = $%d", f.Status)
	}
	whereSQL := strings.Join(where, " AND ")

	var total int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM strategysearch_backtest_run r WHERE `+whereSQL, args...).Scan(&total); err != nil {
		return nil, nil, 0, err
	}

	orderCol := backtestSortColumn(f.SortBy)
	dir := "ASC"
	if f.SortDesc {
		dir = "DESC"
	}
	args = append(args, clampLimit(f.Limit), f.Offset)
	rows, err := pool.Query(ctx, `
		SELECT `+prefixCols("r", backtestRunColList)+`, res.metrics
		FROM strategysearch_backtest_run r
		LEFT JOIN strategysearch_backtest_result res ON res.run_id = r.id
		WHERE `+whereSQL+`
		ORDER BY `+orderCol+` `+dir+` NULLS LAST
		LIMIT $`+itoa(len(args)-1)+` OFFSET $`+itoa(len(args)), args...)
	if err != nil {
		return nil, nil, 0, err
	}
	defer rows.Close()

	outRuns := make([]BacktestRunRow, 0)
	outMetrics := make([]json.RawMessage, 0)
	for rows.Next() {
		var r BacktestRunRow
		var m json.RawMessage
		dst := append(scanBacktestRun(&r), &m)
		if err := rows.Scan(dst...); err != nil {
			return nil, nil, 0, err
		}
		outRuns = append(outRuns, r)
		outMetrics = append(outMetrics, m)
	}
	return outRuns, outMetrics, total, rows.Err()
}

func backtestSortColumn(sortBy string) string {
	switch sortBy {
	case "sharpe", "cagr", "total_return", "max_drawdown", "sqn":
		return "res." + sortBy
	default:
		return "r.created_at"
	}
}

// --- strategysearch_backtest_result ---

type BacktestResultRow struct {
	RunID   string
	Metrics json.RawMessage
}

func GetBacktestResult(ctx context.Context, pool *pgxpool.Pool, runID string) (BacktestResultRow, error) {
	var r BacktestResultRow
	err := pool.QueryRow(ctx, `SELECT run_id, metrics FROM strategysearch_backtest_result WHERE run_id = $1`, runID).Scan(&r.RunID, &r.Metrics)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

// --- helpers ---

const backtestRunColList = "id,strategy_id,spec,spec_hash,uid,interval,period_start,period_end,config,status,error,engine_version,dedup_key,created_at,started_at,finished_at"

var backtestRunCols = backtestRunColList

func scanBacktestRun(r *BacktestRunRow) []any {
	return []any{
		&r.ID, &r.StrategyID, &r.Spec, &r.SpecHash, &r.UID, &r.Interval, &r.PeriodStart, &r.PeriodEnd,
		&r.Config, &r.Status, &r.Error, &r.EngineVersion, &r.DedupKey, &r.CreatedAt, &r.StartedAt, &r.FinishedAt,
	}
}

func prefixCols(alias, cols string) string {
	parts := strings.Split(cols, ",")
	for i, p := range parts {
		parts[i] = alias + "." + strings.TrimSpace(p)
	}
	return strings.Join(parts, ", ")
}
