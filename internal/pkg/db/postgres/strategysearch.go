package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- strategysearch_run ---
// Именование с префиксом StrategySearch* — этот файл живёт в общем пакете
// postgres рядом со strategy.go (домен trb.strategy.v1) и не должно с ним
// коллизировать по идентификаторам.

type StrategySearchRunRow struct {
	ID            string
	Name          string
	BaseSpec      json.RawMessage
	SearchSpace   json.RawMessage
	Study         json.RawMessage
	UID           string
	Interval      int32
	PeriodStart   time.Time
	PeriodEnd     time.Time
	Config        json.RawMessage
	Status        string
	Progress      json.RawMessage
	Error         string
	EngineVersion string
	CreatedAt     time.Time
	StartedAt     *time.Time
	FinishedAt    *time.Time
}

type NewStrategySearchRun struct {
	Name        string
	BaseSpec    json.RawMessage
	SearchSpace json.RawMessage
	Study       json.RawMessage
	UID         string
	Interval    int32
	PeriodStart time.Time
	PeriodEnd   time.Time
	Config      json.RawMessage
	Progress    json.RawMessage
}

func InsertStrategySearchRun(ctx context.Context, pool *pgxpool.Pool, in NewStrategySearchRun) (StrategySearchRunRow, error) {
	var r StrategySearchRunRow
	err := pool.QueryRow(ctx, `
		INSERT INTO strategysearch_run
			(name, base_spec, search_space, study, uid, interval, period_start, period_end, config, progress, status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'queued')
		RETURNING `+strategySearchRunCols,
		in.Name, in.BaseSpec, in.SearchSpace, in.Study, in.UID, in.Interval, in.PeriodStart, in.PeriodEnd, in.Config, in.Progress,
	).Scan(scanStrategySearchRun(&r)...)
	return r, err
}

func GetStrategySearchRun(ctx context.Context, pool *pgxpool.Pool, id string) (StrategySearchRunRow, error) {
	var r StrategySearchRunRow
	err := pool.QueryRow(ctx, `SELECT `+strategySearchRunCols+` FROM strategysearch_run WHERE id = $1`, id).Scan(scanStrategySearchRun(&r)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

func CancelStrategySearchRun(ctx context.Context, pool *pgxpool.Pool, id string) (StrategySearchRunRow, error) {
	var r StrategySearchRunRow
	err := pool.QueryRow(ctx, `
		UPDATE strategysearch_run SET status = 'canceled', finished_at = now()
		WHERE id = $1 AND status IN ('queued','running')
		RETURNING `+strategySearchRunCols, id,
	).Scan(scanStrategySearchRun(&r)...)
	if errors.Is(err, pgx.ErrNoRows) {
		// либо нет строки, либо она уже в терминальном статусе — вернём текущее состояние
		return GetStrategySearchRun(ctx, pool, id)
	}
	return r, err
}

func ListStrategySearchRuns(ctx context.Context, pool *pgxpool.Pool, status string, limit, offset int) ([]StrategySearchRunRow, int, error) {
	where := "true"
	args := []any{}
	if status != "" {
		args = append(args, status)
		where = "status = $1"
	}
	var total int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM strategysearch_run WHERE `+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	args = append(args, clampLimit(limit), offset)
	rows, err := pool.Query(ctx, `SELECT `+strategySearchRunCols+` FROM strategysearch_run WHERE `+where+`
		ORDER BY created_at DESC LIMIT $`+itoa(len(args)-1)+` OFFSET $`+itoa(len(args)), args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := make([]StrategySearchRunRow, 0)
	for rows.Next() {
		var r StrategySearchRunRow
		if err := rows.Scan(scanStrategySearchRun(&r)...); err != nil {
			return nil, 0, err
		}
		out = append(out, r)
	}
	return out, total, rows.Err()
}

// --- strategysearch_trial ---

type StrategySearchTrialRow struct {
	ID              string
	SearchRunID     string
	TrialNumber     int32
	Spec            json.RawMessage
	SpecHash        int64
	Params          json.RawMessage
	Values          json.RawMessage
	State           string
	Metrics         json.RawMessage
	IsParetoOptimal bool
	BacktestRunID   string
	CreatedAt       time.Time
	CompletedAt     *time.Time
}

// ListBestStrategySearchTrials — лучшие завершённые трайлы поиска.
// isMultiObjective=true => возвращает весь фронт Парето (is_pareto_optimal),
// topK игнорируется; иначе — топ-K по values->>metric (единственная цель).
func ListBestStrategySearchTrials(ctx context.Context, pool *pgxpool.Pool, searchID string, topK int, isMultiObjective bool) ([]StrategySearchTrialRow, error) {
	if isMultiObjective {
		rows, err := pool.Query(ctx, `
			SELECT `+strategySearchTrialCols+`
			FROM strategysearch_trial
			WHERE search_run_id = $1 AND state = 'complete' AND is_pareto_optimal
			ORDER BY trial_number`, searchID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return scanStrategySearchTrialRows(rows)
	}
	rows, err := pool.Query(ctx, `
		SELECT `+strategySearchTrialCols+`
		FROM strategysearch_trial
		WHERE search_run_id = $1 AND state = 'complete'
		ORDER BY (
			SELECT value::double precision FROM jsonb_each_text(values) LIMIT 1
		) DESC NULLS LAST
		LIMIT $2`, searchID, clampLimit(topK))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStrategySearchTrialRows(rows)
}

// --- helpers ---

const strategySearchRunCols = "id,name,base_spec,search_space,study,uid,interval,period_start,period_end,config,status,progress,error,engine_version,created_at,started_at,finished_at"

func scanStrategySearchRun(r *StrategySearchRunRow) []any {
	return []any{
		&r.ID, &r.Name, &r.BaseSpec, &r.SearchSpace, &r.Study,
		&r.UID, &r.Interval, &r.PeriodStart, &r.PeriodEnd, &r.Config, &r.Status, &r.Progress, &r.Error, &r.EngineVersion,
		&r.CreatedAt, &r.StartedAt, &r.FinishedAt,
	}
}

const strategySearchTrialCols = "id,search_run_id,trial_number,spec,spec_hash,params,values,state,metrics,is_pareto_optimal,backtest_run_id,created_at,completed_at"

func scanStrategySearchTrial(r *StrategySearchTrialRow) []any {
	return []any{
		&r.ID, &r.SearchRunID, &r.TrialNumber, &r.Spec, &r.SpecHash, &r.Params, &r.Values, &r.State, &r.Metrics,
		&r.IsParetoOptimal, &r.BacktestRunID, &r.CreatedAt, &r.CompletedAt,
	}
}

func scanStrategySearchTrialRows(rows pgx.Rows) ([]StrategySearchTrialRow, error) {
	out := make([]StrategySearchTrialRow, 0)
	for rows.Next() {
		var r StrategySearchTrialRow
		if err := rows.Scan(scanStrategySearchTrial(&r)...); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
