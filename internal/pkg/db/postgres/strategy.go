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

// ErrNotFound — строки нет (обёртка над pgx.ErrNoRows для слоя сервиса).
var ErrNotFound = errors.New("not found")

// --- strategy ---

type StrategyRow struct {
	ID          string
	Name        string
	Description string
	Spec        json.RawMessage
	SpecHash    int64
	SpecVersion int32
	Archived    bool
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func InsertStrategy(ctx context.Context, pool *pgxpool.Pool, name, description string, spec json.RawMessage, specHash int64, specVersion int32) (StrategyRow, error) {
	var r StrategyRow
	err := pool.QueryRow(ctx, `
		INSERT INTO strategy (name, description, spec, spec_hash, spec_version)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, name, description, spec, spec_hash, spec_version, archived, created_at, updated_at`,
		name, description, spec, specHash, specVersion,
	).Scan(&r.ID, &r.Name, &r.Description, &r.Spec, &r.SpecHash, &r.SpecVersion, &r.Archived, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}

func GetStrategy(ctx context.Context, pool *pgxpool.Pool, id string) (StrategyRow, error) {
	var r StrategyRow
	err := pool.QueryRow(ctx, `
		SELECT id, name, description, spec, spec_hash, spec_version, archived, created_at, updated_at
		FROM strategy WHERE id = $1`, id,
	).Scan(&r.ID, &r.Name, &r.Description, &r.Spec, &r.SpecHash, &r.SpecVersion, &r.Archived, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

func ListStrategies(ctx context.Context, pool *pgxpool.Pool, q string, includeArchived bool, limit, offset int) ([]StrategyRow, int, error) {
	limit = clampLimit(limit)
	where := []string{"true"}
	args := []any{}
	if !includeArchived {
		where = append(where, "NOT archived")
	}
	if s := strings.TrimSpace(q); s != "" {
		args = append(args, "%"+s+"%")
		where = append(where, fmt.Sprintf("name ILIKE $%d", len(args)))
	}
	whereSQL := strings.Join(where, " AND ")

	var total int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM strategy WHERE `+whereSQL, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	args = append(args, limit, offset)
	rows, err := pool.Query(ctx, `
		SELECT id, name, description, spec, spec_hash, spec_version, archived, created_at, updated_at
		FROM strategy WHERE `+whereSQL+`
		ORDER BY created_at DESC
		LIMIT $`+itoa(len(args)-1)+` OFFSET $`+itoa(len(args)), args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	out := make([]StrategyRow, 0)
	for rows.Next() {
		var r StrategyRow
		if err := rows.Scan(&r.ID, &r.Name, &r.Description, &r.Spec, &r.SpecHash, &r.SpecVersion, &r.Archived, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, 0, err
		}
		out = append(out, r)
	}
	return out, total, rows.Err()
}

func UpdateStrategy(ctx context.Context, pool *pgxpool.Pool, id, name, description string, spec json.RawMessage, specHash int64, specVersion int32) (StrategyRow, error) {
	var r StrategyRow
	err := pool.QueryRow(ctx, `
		UPDATE strategy
		SET name = $2, description = $3, spec = $4, spec_hash = $5, spec_version = $6, updated_at = now()
		WHERE id = $1
		RETURNING id, name, description, spec, spec_hash, spec_version, archived, created_at, updated_at`,
		id, name, description, spec, specHash, specVersion,
	).Scan(&r.ID, &r.Name, &r.Description, &r.Spec, &r.SpecHash, &r.SpecVersion, &r.Archived, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

func ArchiveStrategy(ctx context.Context, pool *pgxpool.Pool, id string) error {
	tag, err := pool.Exec(ctx, `UPDATE strategy SET archived = true, updated_at = now() WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// SetStrategyArchived переводит стратегию в архив или возвращает из него.
func SetStrategyArchived(ctx context.Context, pool *pgxpool.Pool, id string, archived bool) error {
	tag, err := pool.Exec(ctx, `UPDATE strategy SET archived = $2, updated_at = now() WHERE id = $1`, id, archived)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// --- backtest_run ---

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
	SearchRunID   *string
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
	SearchRunID *string
	DedupKey    *string
}

func InsertBacktestRun(ctx context.Context, pool *pgxpool.Pool, in NewBacktestRun) (BacktestRunRow, error) {
	var r BacktestRunRow
	err := pool.QueryRow(ctx, `
		INSERT INTO backtest_run
			(strategy_id, spec, spec_hash, uid, interval, period_start, period_end, config, search_run_id, dedup_key, status)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'queued')
		RETURNING `+backtestRunCols,
		in.StrategyID, in.Spec, in.SpecHash, in.UID, in.Interval, in.PeriodStart, in.PeriodEnd, in.Config, in.SearchRunID, in.DedupKey,
	).Scan(scanBacktestRun(&r)...)
	return r, err
}

func GetBacktestRun(ctx context.Context, pool *pgxpool.Pool, id string) (BacktestRunRow, error) {
	var r BacktestRunRow
	err := pool.QueryRow(ctx, `SELECT `+backtestRunCols+` FROM backtest_run WHERE id = $1`, id).Scan(scanBacktestRun(&r)...)
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
		FROM backtest_run
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
		UPDATE backtest_run SET status = 'canceled', finished_at = now()
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
	StrategyID  string
	UID         string
	Status      string
	SearchRunID string
	SortBy      string
	SortDesc    bool
	Limit       int
	Offset      int
}

// ListBacktestRuns — прогоны + метрики (LEFT JOIN backtest_result).
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
	if f.SearchRunID != "" {
		add("r.search_run_id = $%d", f.SearchRunID)
	}
	whereSQL := strings.Join(where, " AND ")

	var total int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM backtest_run r WHERE `+whereSQL, args...).Scan(&total); err != nil {
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
		FROM backtest_run r
		LEFT JOIN backtest_result res ON res.run_id = r.id
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

// --- backtest_result ---

type BacktestResultRow struct {
	RunID   string
	Metrics json.RawMessage
}

func GetBacktestResult(ctx context.Context, pool *pgxpool.Pool, runID string) (BacktestResultRow, error) {
	var r BacktestResultRow
	err := pool.QueryRow(ctx, `SELECT run_id, metrics FROM backtest_result WHERE run_id = $1`, runID).Scan(&r.RunID, &r.Metrics)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

// --- search_run ---

type SearchRunRow struct {
	ID             string
	BaseStrategyID *string
	Name           string
	Method         string
	SearchSpace    json.RawMessage
	Structure      json.RawMessage
	Objective      json.RawMessage
	Budget         json.RawMessage
	UID            string
	Interval       int32
	PeriodStart    time.Time
	PeriodEnd      time.Time
	Config         json.RawMessage
	Status         string
	Progress       json.RawMessage
	Error          string
	EngineVersion  string
	CreatedAt      time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
}

type NewSearchRun struct {
	BaseStrategyID *string
	BaseSpec       json.RawMessage
	Name           string
	SearchSpace    json.RawMessage
	Structure      json.RawMessage
	Objective      json.RawMessage
	Budget         json.RawMessage
	UID            string
	Interval       int32
	PeriodStart    time.Time
	PeriodEnd      time.Time
	Config         json.RawMessage
}

func InsertSearchRun(ctx context.Context, pool *pgxpool.Pool, in NewSearchRun) (SearchRunRow, error) {
	var r SearchRunRow
	err := pool.QueryRow(ctx, `
		INSERT INTO search_run
			(base_strategy_id, base_spec, name, method, search_space, structure, objective, budget, uid, interval, period_start, period_end, config, status)
		VALUES ($1,$2,$3,'genetic',$4,$5,$6,$7,$8,$9,$10,$11,$12,'queued')
		RETURNING `+searchRunCols,
		in.BaseStrategyID, in.BaseSpec, in.Name, in.SearchSpace, in.Structure, in.Objective, in.Budget, in.UID, in.Interval, in.PeriodStart, in.PeriodEnd, in.Config,
	).Scan(scanSearchRun(&r)...)
	return r, err
}

func GetSearchRun(ctx context.Context, pool *pgxpool.Pool, id string) (SearchRunRow, error) {
	var r SearchRunRow
	err := pool.QueryRow(ctx, `SELECT `+searchRunCols+` FROM search_run WHERE id = $1`, id).Scan(scanSearchRun(&r)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

func CancelSearchRun(ctx context.Context, pool *pgxpool.Pool, id string) (SearchRunRow, error) {
	var r SearchRunRow
	err := pool.QueryRow(ctx, `
		UPDATE search_run SET status = 'canceled', finished_at = now()
		WHERE id = $1 AND status IN ('queued','running')
		RETURNING `+searchRunCols, id,
	).Scan(scanSearchRun(&r)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return GetSearchRun(ctx, pool, id)
	}
	return r, err
}

func ListSearchRuns(ctx context.Context, pool *pgxpool.Pool, status string, limit, offset int) ([]SearchRunRow, int, error) {
	where := "true"
	args := []any{}
	if status != "" {
		args = append(args, status)
		where = "status = $1"
	}
	var total int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM search_run WHERE `+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	args = append(args, clampLimit(limit), offset)
	rows, err := pool.Query(ctx, `SELECT `+searchRunCols+` FROM search_run WHERE `+where+`
		ORDER BY created_at DESC LIMIT $`+itoa(len(args)-1)+` OFFSET $`+itoa(len(args)), args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	out := make([]SearchRunRow, 0)
	for rows.Next() {
		var r SearchRunRow
		if err := rows.Scan(scanSearchRun(&r)...); err != nil {
			return nil, 0, err
		}
		out = append(out, r)
	}
	return out, total, rows.Err()
}

// --- search_candidate ---

type SearchCandidateRow struct {
	ID            string
	SearchRunID   string
	Spec          json.RawMessage
	SpecHash      int64
	Score         float64
	Metrics       json.RawMessage
	Rank          *int32
	Generation    int32
	BacktestRunID *string
}

func ListSearchCandidates(ctx context.Context, pool *pgxpool.Pool, searchID string, topK int) ([]SearchCandidateRow, error) {
	rows, err := pool.Query(ctx, `
		SELECT id, search_run_id, spec, spec_hash, score, metrics, rank, generation, backtest_run_id
		FROM search_candidate
		WHERE search_run_id = $1 AND status = 'evaluated'
		ORDER BY score DESC
		LIMIT $2`, searchID, clampLimit(topK))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]SearchCandidateRow, 0)
	for rows.Next() {
		var r SearchCandidateRow
		if err := rows.Scan(&r.ID, &r.SearchRunID, &r.Spec, &r.SpecHash, &r.Score, &r.Metrics, &r.Rank, &r.Generation, &r.BacktestRunID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// --- helpers ---

const backtestRunColList = "id,strategy_id,spec,spec_hash,uid,interval,period_start,period_end,config,status,error,engine_version,search_run_id,dedup_key,created_at,started_at,finished_at"

var backtestRunCols = backtestRunColList

func scanBacktestRun(r *BacktestRunRow) []any {
	return []any{
		&r.ID, &r.StrategyID, &r.Spec, &r.SpecHash, &r.UID, &r.Interval, &r.PeriodStart, &r.PeriodEnd,
		&r.Config, &r.Status, &r.Error, &r.EngineVersion, &r.SearchRunID, &r.DedupKey, &r.CreatedAt, &r.StartedAt, &r.FinishedAt,
	}
}

const searchRunCols = "id,base_strategy_id,name,method,search_space,structure,objective,budget,uid,interval,period_start,period_end,config,status,progress,error,engine_version,created_at,started_at,finished_at"

func scanSearchRun(r *SearchRunRow) []any {
	return []any{
		&r.ID, &r.BaseStrategyID, &r.Name, &r.Method, &r.SearchSpace, &r.Structure, &r.Objective, &r.Budget,
		&r.UID, &r.Interval, &r.PeriodStart, &r.PeriodEnd, &r.Config, &r.Status, &r.Progress, &r.Error, &r.EngineVersion,
		&r.CreatedAt, &r.StartedAt, &r.FinishedAt,
	}
}

func prefixCols(alias, cols string) string {
	parts := strings.Split(cols, ",")
	for i, p := range parts {
		parts[i] = alias + "." + strings.TrimSpace(p)
	}
	return strings.Join(parts, ", ")
}

func clampLimit(n int) int {
	if n <= 0 {
		return 50
	}
	if n > 500 {
		return 500
	}
	return n
}

func itoa(n int) string { return fmt.Sprintf("%d", n) }
