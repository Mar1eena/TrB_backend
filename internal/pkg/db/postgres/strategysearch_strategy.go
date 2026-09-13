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

// --- strategysearch_strategy: каталог именованных стратегий ---

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
		INSERT INTO strategysearch_strategy (name, description, spec, spec_hash, spec_version)
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
		FROM strategysearch_strategy WHERE id = $1`, id,
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
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM strategysearch_strategy WHERE `+whereSQL, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	args = append(args, limit, offset)
	rows, err := pool.Query(ctx, `
		SELECT id, name, description, spec, spec_hash, spec_version, archived, created_at, updated_at
		FROM strategysearch_strategy WHERE `+whereSQL+`
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
		UPDATE strategysearch_strategy
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

// SetStrategyArchived переводит стратегию в архив или возвращает из него.
func SetStrategyArchived(ctx context.Context, pool *pgxpool.Pool, id string, archived bool) error {
	tag, err := pool.Exec(ctx, `UPDATE strategysearch_strategy SET archived = $2, updated_at = now() WHERE id = $1`, id, archived)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
